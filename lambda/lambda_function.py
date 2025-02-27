import asyncio
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Dict, Tuple

# Import PDF processing libraries
import PyPDF2

# AWS libraries
import aioboto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer()

# Initialize session at module level
session = aioboto3.Session()


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


class DocumentProcessor:
    VALID_VISA_CATEGORIES = {"EB1A", "EB2-NIW"}
    
    def __init__(self, visa_category: str):
        if visa_category not in self.VALID_VISA_CATEGORIES:
            raise ValueError(f"Invalid visa category. Must be one of: {self.VALID_VISA_CATEGORIES}")
        
        self.visa_category = visa_category
        self.kb_id = "BYASZZZFRM"
        logger.info(f"Initialized DocumentProcessor for {self.visa_category}")

    async def _invoke_bedrock_model(self, request_body: Dict) -> Dict:
        """Invoke Bedrock model with async client"""
        async with session.client("bedrock-runtime") as bedrock:
            response = await bedrock.invoke_model(
                modelId="anthropic.claude-v2:1",
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body),
            )
            response_body = json.loads(await response["body"].read())
            completion = response_body.get("completion", "").strip()
            return json.loads(self._extract_json(completion))

    async def _evaluate_against_criteria(self, text: str) -> Dict:
        """
        Directly evaluate the profile against USCIS eligibility criteria by querying the model
        
        For EB1A, we need to meet at least 3 out of 10 criteria.
        For EB2-NIW, we evaluate against the NYSDOT framework.
        """
        try:
            start_time = time.time()
            logger.info(f"Starting criteria evaluation for {self.visa_category}")
            
            if self.visa_category == "EB1A":
                # EB1A has 10 specific criteria, need to meet at least 3
                criteria_list = [
                    "nationally or internationally recognized prizes or awards for excellence",
                    "membership in associations that require outstanding achievement",
                    "published material about the alien in professional publications",
                    "judging the work of others in the field",
                    "original scientific, scholarly, or business-related contributions of major significance",
                    "authorship of scholarly articles in professional journals or major media",
                    "display of work at artistic exhibitions or showcases",
                    "performing a leading or critical role for distinguished organizations",
                    "high salary or remuneration compared to others in the field",
                    "commercial success in the performing arts"
                ]
                
                # Create a structured prompt for evaluation
                eval_prompt = (
                    f"\n\nHuman: As an immigration expert specializing in EB1A petitions, "
                    f"evaluate this resume against the USCIS criteria for Extraordinary Ability. "
                    f"For each criterion, determine if the evidence is Strong, Moderate, Weak, or Not Present. "
                    f"Then indicate which criteria are likely to be approved by USCIS based on the evidence. "
                    f"A strong petition should meet at least 3 of the 10 criteria.\n\n"
                    f"The 10 criteria are:\n"
                    f"{', '.join(criteria_list)}\n\n"
                    f"Resume text:\n{text}\n\n"
                    f"Return your evaluation in JSON format with the following structure ONLY:\n"
                    f'{{\n'
                    f'  "criteria_evaluation": {{\n'
                    f'    "criterion1_name": {{\n'
                    f'      "evidence_level": "Strong|Moderate|Weak|Not Present",\n'
                    f'      "evidence_found": "Description of evidence found",\n'
                    f'      "likely_approved": true|false\n'
                    f'    }},\n'
                    f'    // repeat for all 10 criteria\n'
                    f'  }},\n'
                    f'  "met_criteria_count": 0-10,\n'
                    f'  "threshold_met": true|false,\n'
                    f'  "strongest_criteria": ["criterion1", "criterion2"],\n'
                    f'  "suggested_improvements": ["improvement1", "improvement2"]\n'
                    f'}}\n\n'
                    f"Assistant:"
                )
                
            else:  # EB2-NIW
                # EB2-NIW evaluates against the NYSDOT framework (three prongs)
                eval_prompt = (
                    f"\n\nHuman: As an immigration expert specializing in EB2-NIW petitions, "
                    f"evaluate this resume against the NYSDOT three-prong test for National Interest Waiver. "
                    f"For each prong, determine if the evidence is Strong, Moderate, Weak, or Not Present. "
                    f"Then provide an overall assessment.\n\n"
                    f"The three prongs are:\n"
                    f"1. The foreign national's proposed endeavor has substantial merit and national importance\n"
                    f"2. The foreign national is well positioned to advance the proposed endeavor\n"
                    f"3. On balance, it would be beneficial to the United States to waive the job offer and labor certification requirements\n\n"
                    f"Resume text:\n{text}\n\n"
                    f"Return your evaluation in JSON format with the following structure ONLY:\n"
                    f'{{\n'
                    f'  "prong_evaluation": {{\n'
                    f'    "prong1": {{\n'
                    f'      "evidence_level": "Strong|Moderate|Weak|Not Present",\n'
                    f'      "evidence_found": "Description of evidence found",\n'
                    f'      "likely_approved": true|false\n'
                    f'    }},\n'
                    f'    "prong2": {{\n'
                    f'      "evidence_level": "Strong|Moderate|Weak|Not Present",\n'
                    f'      "evidence_found": "Description of evidence found",\n'
                    f'      "likely_approved": true|false\n'
                    f'    }},\n'
                    f'    "prong3": {{\n'
                    f'      "evidence_level": "Strong|Moderate|Weak|Not Present",\n'
                    f'      "evidence_found": "Description of evidence found",\n'
                    f'      "likely_approved": true|false\n'
                    f'    }}\n'
                    f'  }},\n'
                    f'  "all_prongs_met": true|false,\n'
                    f'  "strongest_evidence": ["evidence1", "evidence2"],\n'
                    f'  "suggested_improvements": ["improvement1", "improvement2"]\n'
                    f'}}\n\n'
                    f"Assistant:"
                )
            
            # Create request body
            request_body = {
                "prompt": eval_prompt,
                "max_tokens_to_sample": 3000,
                "temperature": 0.1,
                "top_k": 250,
                "top_p": 1,
                "stop_sequences": ["\n\nHuman:"],
                "anthropic_version": "bedrock-2023-05-31",
            }

            # Invoke model for evaluation
            evaluation = await self._invoke_bedrock_model(request_body)
            
            total_time = time.time() - start_time
            logger.info(f"Completed criteria evaluation in {total_time:.3f} seconds")
            
            # Add visa category to result
            evaluation["visa_category"] = self.visa_category
            
            return evaluation

        except Exception as e:
            logger.error(f"Error evaluating against criteria: {str(e)}")
            
            # Return default structure based on visa category
            if self.visa_category == "EB1A":
                return {
                    "visa_category": self.visa_category,
                    "criteria_evaluation": {},
                    "met_criteria_count": 0,
                    "threshold_met": False,
                    "strongest_criteria": [],
                    "suggested_improvements": ["Unable to evaluate due to an error"],
                    "error": str(e)
                }
            else:  # EB2-NIW
                return {
                    "visa_category": self.visa_category,
                    "prong_evaluation": {},
                    "all_prongs_met": False,
                    "strongest_evidence": [],
                    "suggested_improvements": ["Unable to evaluate due to an error"],
                    "error": str(e)
                }
    
    def _extract_text_from_pdf(self, file_content: bytes) -> str:
        """Optimized PDF text extraction with time logging"""
        start_time = time.time()
        logger.info(f"Starting PDF extraction for {self.visa_category} visa category")
        
        try:
            # Process PDF directly from memory
            pdf_file = BytesIO(file_content)
            
            # Log file size
            pdf_size_kb = len(file_content) / 1024
            logger.info(f"Processing PDF of size: {pdf_size_kb:.2f} KB")
            
            # Start page extraction timer
            page_start_time = time.time()
            reader = PyPDF2.PdfReader(pdf_file)
            page_count = len(reader.pages)
            logger.info(f"PDF has {page_count} pages, reader initialized in {(time.time() - page_start_time):.3f} seconds")
            
            text_parts = []
            
            # Extract text from each page
            for i in range(page_count):
                page_start = time.time()
                page = reader.pages[i]
                text = page.extract_text()
                
                if text:
                    text_parts.append(text)
                    
                # Log every 5 pages for larger documents
                if page_count > 10 and (i + 1) % 5 == 0:
                    logger.info(f"Processed {i+1}/{page_count} pages ({((i+1)/page_count*100):.1f}%) in {(time.time() - page_start):.3f}s")
            
            # Join all text at once
            joining_start = time.time()
            full_text = "\n".join(text_parts)
            logger.debug(f"Text joining completed in {(time.time() - joining_start):.3f} seconds")
            
            if not full_text.strip():
                raise ValueError("No text content extracted from PDF")
            
            # Log total extraction time
            total_time = time.time() - start_time
            logger.info(f"PDF extraction completed in {total_time:.3f} seconds for {page_count} pages ({page_count/total_time:.2f} pages/second)")
            
            return full_text.strip()
            
        except Exception as e:
            # Log failure time
            total_time = time.time() - start_time
            logger.error(f"PDF extraction failed after {total_time:.3f} seconds with error: {str(e)}")
            raise ValueError(f"Failed to extract text from PDF: {str(e)}")

    def _extract_json(self, text: str) -> str:
        """Optimized JSON extraction"""
        if "```json" in text:
            return text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            json_block = [
                block.strip()
                for block in text.split("```")
                if "{" in block and "}" in block
            ]
            if json_block:
                return json_block[0]

        json_start = text.find("{")
        json_end = text.rfind("}") + 1
        if json_start >= 0 and json_end > 0:
            return text[json_start:json_end]

        raise ValueError("No JSON content found in response")

    @tracer.capture_method
    async def process_document(self, file_content: bytes) -> Dict:
        """Process document with direct criteria evaluation only"""
        overall_start = time.time()
        
        try:
            # Extract text from PDF
            pdf_start = time.time()
            text = self._extract_text_from_pdf(file_content)
            pdf_time = time.time() - pdf_start
            logger.info(f"Extracted PDF text in {pdf_time:.3f} seconds")
            
            # Directly evaluate against criteria
            criteria_start = time.time()
            criteria_analysis = await self._evaluate_against_criteria(text)
            criteria_time = time.time() - criteria_start
            logger.info(f"Completed criteria evaluation in {criteria_time:.3f} seconds")
            
            # Calculate success probability based on criteria evaluation
            if self.visa_category == "EB1A":
                met_count = criteria_analysis.get("met_criteria_count", 0)
                threshold_met = criteria_analysis.get("threshold_met", False)
                
                if threshold_met and met_count >= 5:
                    probability = "High"
                    explanation = f"Strong case meeting {met_count} criteria (only 3 required)"
                elif threshold_met:
                    probability = "Moderate"
                    explanation = f"Meets minimum threshold of 3 criteria with {met_count} criteria satisfied"
                elif met_count >= 2:
                    probability = "Low"
                    explanation = f"Nearly meets threshold with {met_count} criteria (3 required)"
                else:
                    probability = "Very Low"
                    explanation = f"Only meets {met_count} criteria out of 3 required"
            else:
                # For EB2-NIW, check if all prongs are met
                all_prongs_met = criteria_analysis.get("all_prongs_met", False)
                
                if all_prongs_met:
                    probability = "High"
                    explanation = "All three prongs of NYSDOT test are satisfied"
                else:
                    # Count how many prongs are met
                    prong_eval = criteria_analysis.get("prong_evaluation", {})
                    met_prongs = sum(1 for p in prong_eval.values() if p.get("likely_approved", False))
                    
                    if met_prongs == 2:
                        probability = "Moderate"
                        explanation = "Two of three prongs are satisfied, but one needs strengthening"
                    elif met_prongs == 1:
                        probability = "Low"
                        explanation = "Only one prong is strongly satisfied"
                    else:
                        probability = "Very Low"
                        explanation = "None of the three prongs is sufficiently satisfied"
            
            # Total processing time
            total_time = time.time() - overall_start
            logger.info(f"Complete document processing finished in {total_time:.3f} seconds")
            
            # Include timing data in the result
            timing_data = {
                "pdf_extraction_time": round(pdf_time, 3),
                "criteria_evaluation_time": round(criteria_time, 3),
                "total_processing_time": round(total_time, 3),
            }

            return {
                "visa_category": self.visa_category,
                "criteria_analysis": criteria_analysis,
                "success_probability": {
                    "rating": probability,
                    "explanation": explanation
                },
                "timing_data": timing_data,
            }

        except Exception as e:
            # Log error with timing information
            total_time = time.time() - overall_start
            logger.error(f"Error processing document after {total_time:.3f} seconds: {str(e)}")
            raise


async def async_lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Async Lambda handler with timing metrics"""
    request_start_time = time.time()
    
    request_id = context.aws_request_id
    logger.info(f"Starting request processing for request ID: {request_id}")
    
    try:
        # Parse multipart form data
        form_data_start = time.time()
        form_data = parse_multipart(event)
        logger.info(f"Form data parsing completed in {(time.time() - form_data_start):.3f} seconds")

        # Get visa category and file content
        visa_category = form_data.get("visa_category", "EB1A").strip('"')
        file_content = form_data.get("file")
        
        if file_content:
            file_size_kb = len(file_content) / 1024
            logger.info(f"Received file of size: {file_size_kb:.2f} KB for {visa_category} category")
        else:
            raise ValueError("No file provided")

        if visa_category not in DocumentProcessor.VALID_VISA_CATEGORIES:
            raise ValueError(
                f"Invalid visa category. Must be one of: {DocumentProcessor.VALID_VISA_CATEGORIES}"
            )

        # Process document
        processor = DocumentProcessor(visa_category)
        processing_start = time.time()
        result = await processor.process_document(file_content)
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
    """Main Lambda handler that runs the async handler"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(async_lambda_handler(event, context))