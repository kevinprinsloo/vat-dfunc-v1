import azure.functions as func
import azure.durable_functions as df
import logging
import os
import base64
from PIL import Image, ImageOps
from io import BytesIO
import fitz  # PyMuPDF for PDF rendering
from openai import AzureOpenAI
import json
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueServiceClient
import asyncio
from typing import Optional, List, Dict

# Initialize DFApp for Durable Functions
app = df.DFApp()

# Azure OpenAI configuration
FUNC_AZURE_OPENAI_ENDPOINT = os.getenv("FUNC_AZURE_OPENAI_ENDPOINT")
FUNC_AZURE_OPENAI_API_KEY = os.getenv("FUNC_AZURE_OPENAI_API_KEY")
FUNC_GPT_DEPLOYMENT_NAME = os.getenv("FUNC_GPT_DEPLOYMENT_NAME")
AZURE_STORAGE_CONNECTION_STRING= os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Initialize the Azure OpenAI client
client = AzureOpenAI(
    azure_endpoint=FUNC_AZURE_OPENAI_ENDPOINT,
    api_key=FUNC_AZURE_OPENAI_API_KEY,
    api_version="2024-08-01-preview"
)

SYSTEM_MESSAGE = "You are an expert tax specialist tasked with extracting tax information from receipts and invoices."
PROMPT = """
You are an expert tax specialist with accountancy experience whose task is to determine if the provided receipt or invoice has a VAT number and if we can claim VAT back.  
Identify the reference on the invoice or receipt, if it is has a VAT reference (or equivalence) in which case extract the VAT number and VAT total and set VAT as TRUE. 
If there are problems such as not able to read the image, say "NA"

You are provided with an image of a receipt.
Your task is to determine if VAT (Value Added Tax), or V.A.T or any equivalent tax (e.g., sales tax) is applied on the receipt and extract the VAT/tax amount if it is mentioned.
Additionally, consider scenarios where VAT is not explicitly mentioned but could be included in the total price (e.g., in regions where VAT is generally included).
Provide your response in JSON format, including the following fields:

1. VAT_applied: A boolean indicating if VAT or a similar tax (like sales tax) was applied (true/false).
2. VAT_amount: The total amount of VAT or tax applied, or null if no VAT is applied or mentioned.
3. Currency: The currency used in the receipt (e.g., USD, GBP, etc.), or null if unknown.
4. confidence: A value between 0 and 1 indicating how confident you are in your VAT/tax classification and extraction.
5. The VAT, V.A.T or equivalent tax number if present, might be displayed below the VAT title on the next line. Extract this number if present, otherwise set it to null.

Export in the following JSON format, no explanations needed:

{
  "Receipt1": {
    "reference": "234532785",
    "supplier": "Acme ltd",
    "vat": true,
    "vatNumber": 123456789,
    "vatTotal": "£40",
    "currency": "GBP",
    "date": "2024-10-17",
    "total": "£200"
  },
  "Receipt2": {
    "reference": "1234653",
    "supplier": "Widgets ltd",
    "vat": false,
    "vatNumber": null,
    "vatTotal": null,
    "currency": "GBP",
    "date": "2024-10-17",
    "total": "£200"
  }
}
"""

# Blob trigger for PDF processing
@app.function_name(name="blob_trigger_input")
@app.blob_trigger(arg_name="myblob", path="blobtriggerinput/{name}", connection="AZURE_STORAGE_CONNECTION_STRING")
def blob_trigger_input(myblob: func.InputStream):
    logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")

    if not myblob.name.lower().endswith(".pdf"):
        logging.warning("Skipping non-PDF blob.")
        return

    # Extract only the blob name
    blob_name = os.path.basename(myblob.name)

    enqueue_message(
        queue_name="orchestrationqueue",
        message={"name": blob_name},
        connection_string=AZURE_STORAGE_CONNECTION_STRING
    )

def enqueue_message(queue_name: str, message: dict, connection_string: str):
    try:
        queue_service = QueueServiceClient.from_connection_string(connection_string)
        queue_client = queue_service.get_queue_client(queue_name)

        json_message = json.dumps(message)
        # Base64 encode the message
        encoded_message = base64.b64encode(json_message.encode('utf-8')).decode('utf-8')

        queue_client.send_message(encoded_message)
        logging.info(f"Enqueued message to queue '{queue_name}': {message}")
    except Exception as e:
        logging.error(f"Failed to enqueue message: {e}")




# Durable Function orchestrator
@app.function_name(name="hello_orchestrator")
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input()
    logging.info(f"Orchestrator started with input: {input_data}")

    try:
        result = yield context.call_activity("process_pdf", input_data)
        logging.info(f"Orchestrator completed successfully: {result}")
        return {"status": "completed", "result": result}
    except Exception as e:
        logging.error(f"Orchestrator failed: {e}", exc_info=True)
        return {"status": "failed", "reason": str(e)}


# Starter Function
@app.function_name(name="start_orchestrator")
@app.queue_trigger(arg_name="message", queue_name="orchestrationqueue", connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
async def start_orchestrator(message: func.QueueMessage, client):
    logging.info(f"Received queue message: {message.id}")

    try:
        message_body = message.get_body().decode('utf-8')
        logging.debug(f"Message body: {message_body}")
        input_data = json.loads(message_body)
        instance_id = await client.start_new("hello_orchestrator", None, input_data)
        logging.info(f"Orchestration started with ID = '{instance_id}'")
    except Exception as e:
        logging.error(f"Failed to start orchestration: {e}", exc_info=True)



# Activity Function
@app.function_name(name="process_pdf")
@app.activity_trigger(input_name="inputData")
def process_pdf(inputData: dict) -> dict:
    pdf_name = inputData.get("name")
    logging.info(f"Processing PDF: {pdf_name}")

    try:
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_name = "blobtriggerinput"  # The container where the PDFs are stored
        
        # Use the pdf_name directly as the blob name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=pdf_name)
        pdf_content = blob_client.download_blob().readall()

        images = render_pdf_to_images(pdf_content, pdf_name)

        if not images:
            raise ValueError("No images extracted from the PDF.")

        results = []
        for image in images:
            image_base64 = f"data:image/{image['format']};base64,{image['data']}"
            result = process_receipt_with_gpt(image_base64)
            results.append(result)

        final_result = {"status": "success", "results": results}
    except Exception as e:
        logging.error(f"Error processing PDF '{pdf_name}': {e}", exc_info=True)
        final_result = {"status": "error", "reason": str(e)}
    finally:
        save_results_to_blob(pdf_name, final_result)

    return final_result



def save_results_to_blob(blob_name: str, result: dict):
    """Saves the result of processing to Azure Blob Storage asynchronously"""
    try:
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_name = "processedblobtriggerresults"
        container_client = blob_service_client.get_container_client(container_name)

        # Attempt to create the container
        try:
            container_client.create_container()
            logging.info(f"Created container '{container_name}'.")
        except ResourceExistsError:
            logging.info(f"Container '{container_name}' already exists.")
        except Exception as e:
            logging.error(f"Failed to create container '{container_name}': {e}", exc_info=True)
            return  # Exit if container creation failed for reasons other than existence

        # Proceed to upload the blob
        result_blob_name = f"{blob_name}_results.json"
        result_data = json.dumps(result, indent=4)
        blob_client = container_client.get_blob_client(result_blob_name)
        blob_client.upload_blob(result_data, overwrite=True)
        logging.info(f"Saved results to blob '{result_blob_name}' in container '{container_name}'.")

    except Exception as e:
        logging.error(f"Failed to save results to blob: {e}", exc_info=True)


def render_pdf_to_images(pdf_bytes: bytes, pdf_name: str) -> Optional[List[Dict[str, str]]]:
    """Renders a PDF file into images and returns a list of Base64-encoded images."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        images = []
        for page_number in range(len(doc)):
            page = doc.load_page(page_number)
            pix = page.get_pixmap(dpi=200)  # Render the page as an image

            # Log original page dimensions
            logging.debug(f"Page {page_number + 1} dimensions: {page.rect.width}x{page.rect.height}")

            # Get image bytes and open in PIL
            img_bytes = pix.tobytes("png")
            image = Image.open(BytesIO(img_bytes))

            # Optional: Add padding
            image = add_padding(image, padding=10)

            # Resize for consistency
            image = resize_image(image)

            # Ensure no truncation occurs
            if image.size[0] < 100 or image.size[1] < 100:
                raise ValueError(f"Truncated image detected on page {page_number + 1}: {image.size}")

            # Convert to JPEG
            buffered = BytesIO()
            image.save(buffered, format="JPEG", optimize=True, quality=75)
            jpeg_bytes = buffered.getvalue()

            # Encode to Base64
            encoded_image = {
                "data": base64.b64encode(jpeg_bytes).decode("utf-8"),
                "format": "jpeg",
                "page": page_number + 1,
                "pdf_name": pdf_name
            }
            images.append(encoded_image)

        return images
    except Exception as e:
        logging.error(f"Error rendering PDF '{pdf_name}' to images: {e}")
        return None


def resize_image(image: Image.Image, max_dimension: int = 2048, min_dimension: int = 768) -> Image.Image:
    """Resizes an image while maintaining aspect ratio"""
    try:
        width, height = image.size
        logging.debug(f"Original image size: {width}x{height}")

        resample_filter = Image.LANCZOS if hasattr(Image, 'LANCZOS') else Image.ANTIALIAS

        scaling_factor = min(max_dimension / max(width, height), 1.0)

        if scaling_factor < 1.0:
            new_width = int(width * scaling_factor)
            new_height = int(height * scaling_factor)
            image = image.resize((new_width, new_height), resample_filter)
            logging.debug(f"Resized image to: {new_width}x{new_height}")

        width, height = image.size
        if min(width, height) > min_dimension:
            scaling_factor = min_dimension / min(width, height)
            new_width = int(width * scaling_factor)
            new_height = int(height * scaling_factor)
            image = image.resize((new_width, new_height), resample_filter)
            logging.debug(f"Further resized image to fit minimum dimensions: {new_width}x{new_height}")

        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
            logging.debug("Converted image to RGB mode.")

        return image
    except Exception as e:
        logging.error(f"Error resizing image: {e}")
        return image


def add_padding(image: Image.Image, padding: int = 10) -> Image.Image:
    """Adds padding to an image."""
    try:
        return ImageOps.expand(image, border=padding, fill="white")
    except Exception as e:
        logging.error(f"Error adding padding to image: {e}")
        return image


def process_receipt_with_gpt(image_base64: str):
    """Sends a Base64-encoded image to Azure GPT-4 API and retrieves receipt information."""
    try:
        if not image_base64.startswith("data:image/jpeg;base64,"):
            raise ValueError("Invalid image format. Expected Base64-encoded JPEG string.")

        # Prepare the GPT API request
        messages = [
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": PROMPT},
            {"role": "user", "content": f"Image: {image_base64}"}
        ]

        # Call Azure OpenAI API
        response = client.chat.completions.create(
            model=FUNC_GPT_DEPLOYMENT_NAME,
            messages=messages,
            max_tokens=800,
            temperature=0.0
        )

        # Extract response content
        content = response.choices[0].message.content
        logging.debug(f"GPT-4 response: {content}")
        return content

    except Exception as e:
        logging.error(f"Error processing image with GPT-4: {e}")
        return {"error": str(e)}