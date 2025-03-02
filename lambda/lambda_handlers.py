import base64
import json
import os
import time
from typing import Dict

# AWS libraries
import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext

# Import local modules
from eb1a_processor import DocumentProcessor

logger = Logger()
tracer = Tracer()

def parse_multipart(event: dict) -> Dict:
    """Parse multipart/form-data from API Gateway event"""
    try:
        body = event.get("body", "")
        logger.debug(f"Raw event body type: {type(body)}")
        logger.debug(f"isBase64Encoded: {event.get('isBase64Encoded', False)}")

        if event.get("isBase64Encoded", False):
            try:
                body = base64.b64decode(body)
            except Exception as e:
                logger.error(f"Base64 decode error: {str(e)}")
                raise ValueError(f"Failed to decode base64 body: {str(e)}")

        if isinstance(body, str):
            body = body.encode("utf-8")

        headers = {k.lower(): v for k, v in event.get("headers", {}).items()}
        content_type = headers.get("content-type", "")

        if not content_type.startswith("multipart/form-data"):
            raise ValueError("Invalid content type. Expected multipart/form-data")

        boundary = None
        for part in content_type.split(";"):
            if "boundary" in part:
                boundary = part.split("=")[1].strip().strip("\"'")
                break

        if not boundary:
            raise ValueError("No boundary found in content type")

        parts = body.split(b"--" + boundary.encode("utf-8"))
        form_data = {}

        for part in parts:
            if not part.strip() or part.strip() == b"--":
                continue

            try:
                headers_raw, content = part.split(b"\r\n\r\n", 1)
                headers_raw = headers_raw.decode("utf-8")

                headers = {}
                for line in headers_raw.split("\r\n"):
                    if ":" in line:
                        key, value = line.split(":", 1)
                        headers[key.strip().lower()] = value.strip()

                content_disposition = headers.get("content-disposition", "")
                field_name = None
                filename = None

                for item in content_disposition.split(";"):
                    item = item.strip()
                    if item.startswith("name="):
                        field_name = item[5:].strip("\"'")
                    elif item.startswith("filename="):
                        filename = item[9:].strip("\"'")

                if field_name:
                    form_data[field_name] = (
                        content[:-2] if filename else content[:-2].decode("utf-8")
                    )

            except Exception as e:
                logger.error(f"Error parsing part: {str(e)}")
                continue

        return form_data

    except Exception as e:
        logger.error(f"Error parsing multipart/form-data: {str(e)}")
        raise ValueError(f"Failed to parse multipart/form-data: {str(e)}")

def lambda_handler_function(event: dict, context: LambdaContext) -> dict:
    """ Lambda handler with timing metrics"""
    request_start_time = time.time()
    
    request_id = context.aws_request_id
    logger.info(f"Starting request processing for request ID: {request_id}")
    
    try:
        # Parse multipart form data
        form_data_start = time.time()
        form_data = parse_multipart(event)
        logger.info(f"Form data parsing completed in {(time.time() - form_data_start):.3f} seconds")

        # Get file content (visa category is hardcoded to EB1A now)
        file_content = form_data.get("file")
        
        if file_content:
            file_size_kb = len(file_content) / 1024
            logger.info(f"Received file of size: {file_size_kb:.2f} KB for EB1A visa assessment")
        else:
            raise ValueError("No file provided")

        # Process document
        processor = DocumentProcessor()  # Default is EB1A now
        processing_start = time.time()
        result = processor.process_document(file_content)
        processing_time = time.time() - processing_start
        logger.info(f"Document processing completed in {processing_time:.3f} seconds")

        # Calculate total request time
        total_request_time = time.time() - request_start_time
        logger.info(f"Total request processing time: {total_request_time:.3f} seconds for request ID: {request_id}")
        
        # Add timing data to response
        result["processing_metadata"] = {
            "total_processing_time_seconds": round(total_request_time, 3),
            "request_id": request_id
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(result),
        }

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        
        # Log total time for failed requests too
        total_request_time = time.time() - request_start_time
        logger.info(f"Failed request completed in {total_request_time:.3f} seconds")
        
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({
                "error": str(e),
                "processing_time_seconds": round(total_request_time, 3),
                "request_id": request_id
            }),
        }

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        
        # Log total time for failed requests too
        total_request_time = time.time() - request_start_time
        logger.info(f"Failed request completed in {total_request_time:.3f} seconds")
        
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({
                "error": str(e),
                "processing_time_seconds": round(total_request_time, 3),
                "request_id": request_id
            }),
        }


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Main Lambda handler that runs the handler"""
    return lambda_handler_function(event, context)