import base64
import json
import os
import time
from io import BytesIO
from typing import Dict, Tuple, List, Any

# Import PDF processing libraries
import PyPDF2

# AWS libraries
import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext

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


class DocumentProcessor:
    VALID_VISA_CATEGORIES = {"EB1A"}
    
    def __init__(self, visa_category: str = "EB1A"):
        if visa_category not in self.VALID_VISA_CATEGORIES:
            raise ValueError(f"Invalid visa category. Only EB1A is supported.")
        
        self.visa_category = "EB1A"  # Hardcode to EB1A as we're only supporting this
        self.kb_id = os.environ.get("KNOWLEDGE_BASE_ID", "BYASZZZFRM")
        logger.info(f"Initialized DocumentProcessor for EB1A with KB ID: {self.kb_id}")

    def _retrieve_and_generate(self, query: str, text_context: str) -> Dict:
        """
        Use the Bedrock RetrieveAndGenerate API to perform RAG in a single call
        This combines retrieval from KB with generation using Claude
        """
        start_time = time.time()
        logger.info("Starting RetrieveAndGenerate for EB1A eligibility assessment")
    
        try:
            # Create prompt for EB1A assessment without listing the criteria
            prompt = (
                f"As an immigration expert specializing in EB1A petitions, "
                f"evaluate this resume against all 10 USCIS criteria for Extraordinary Ability "
                f"as defined in the USCIS Policy Manual and regulations. "
                f"For each of the 10 criteria, determine if the evidence is Strong, Moderate, Weak, or Not Present. "
                f"Then indicate which criteria are likely to be approved by USCIS based on the evidence. "
                f"Use the latest USCIS guidance and policy documents from the knowledge base to make your assessment. "
                f"Remember that a strong petition should meet at least 3 of the 10 criteria.\n\n"
                f"Resume text:\n{text_context}\n\n"
                f"Return your evaluation in JSON format with the following structure ONLY:\n"
                f'{{\n'
                f'  "criteria_evaluation": {{\n'
                f'    "criterion_name": {{\n'
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
                f'}}'
            )
        
          
            # Define custom prompt templates for Claude 3.5 models
            orchestration_prompt = (
                "Human: $conversation_history$\n"
                "Please retrieve information  Knowledge Base:\n"
                "1. Example EB1A petitions that were rejected and appealed\n\n"
                "When analyzing the rejected petitions, look for common weaknesses and patterns that led to their rejection."
                "This information will be used to evaluate a resume against EB1A criteria, "
                "identifying potential vulnerabilities that might lead to rejection.\n\n"
                "$output_format_instructions$\n\n"
                "Assistant:"
            )
            
            generation_prompt = (
                "Human: I need to evaluate this resume for an EB1A visa petition.\n\n"
                "The knowledge base contains information about rejected appealed petitions:\n"
                "- Real examples of EB1A petitions that were rejected and appealed\n\n"
                "$search_results$\n\n"
                "Using both the official criteria and the rejected petition examples, provide a comprehensive evaluation "
                "of the following resume. Specifically:\n"
                "1. Evaluate each EB1A criterion based on the resume content\n"
                "2. Identify any similarities between this resume and rejected petitions that might make certain criteria less likely to be approved\n"
                "3. Highlight strengths and potential vulnerabilities in the petition\n"
                "4. Suggest specific improvements to strengthen the case based on patterns seen in rejected petitions\n\n"
                "Resume to evaluate:\n{text_context}\n\n"
                "Assistant:"
            )
            
            
            bedrock_agent = boto3.client('bedrock-agent-runtime')
            response =  bedrock_agent.retrieve_and_generate(
                    input={
                        "text": prompt
                    },
                    retrieveAndGenerateConfiguration={
                        "type": "KNOWLEDGE_BASE",
                        "knowledgeBaseConfiguration": {
                            "knowledgeBaseId": self.kb_id,
                            "modelArn": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                            "retrievalConfiguration": {
                                "vectorSearchConfiguration": {
                                    "numberOfResults": 5,
                                    "filter": {
                                        "andAll": [
                                            {
                                                "equals": {
                                                    "key": "petition_type",
                                                    "value": "EB1A"
                                                }
                                            },
                                            {
                                                "equals": {
                                                    "key": "petition_status",
                                                    "value": "rejected_appealed"
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            # Fixed orchestrationConfiguration
                            "orchestrationConfiguration": {
                                "promptTemplate": {
                                    "textPromptTemplate": orchestration_prompt
                                },
                                "inferenceConfig": {
                                    "textInferenceConfig": {
                                        "temperature": 0.1,
                                        "maxTokens": 4000,
                                        "topP": 0.9
                                    }
                                }
                            },
                            # Fixed generationConfiguration
                            "generationConfiguration": {
                                "promptTemplate": {
                                    "textPromptTemplate": generation_prompt
                             },
                                "inferenceConfig": {
                                    "textInferenceConfig": {
                                        "temperature": 0.1,
                                        "maxTokens": 4000,
                                        "topP": 0.9
                                    }
                                }
                            }
                        }
                    }
            )
            
            # Extract the generated response
            generation = response.get("output", {}).get("text", "")
            citations = response.get("citations", [])
                
            # Log citation sources - this is the new code
            logger.info(f"Raw response data: {str(response)}")
            logger.info(f"Raw response JSON data: {json.dumps(response)}")
            
        
            # Extract and parse JSON from the generation
            json_result = self._extract_json(generation)
            parsed_result = json.loads(json_result)
            
            # Add citation information with source tracking
            parsed_result["citations"] = [
                {
                    "text": citation.get("generatedResponsePart", {}).get("textResponsePart", {}).get("text", ""),
                    "source": citation.get("retrievedReferences", [{}])[0].get("content", {}).get("text", "")[:200] + "..." if citation.get("retrievedReferences") else "",
                    "source_location": citation.get("retrievedReferences", [{}])[0].get("location", {}).get("type", "") if citation.get("retrievedReferences") else ""
                }
                for citation in citations
            ]
        
            
    
            # Add visa category to result
            parsed_result["visa_category"] = self.visa_category
                
        
            # Log completion time
            total_time = time.time() - start_time
            logger.info(f"RetrieveAndGenerate completed in {total_time:.3f} seconds")
            
            return parsed_result
        
        except (json.JSONDecodeError, ValueError) as e:
            # Handle issues with JSON parsing or extraction
            logger.error(f"Error parsing response from RAG: {str(e)}")
            logger.warning("Falling back to direct model invocation due to JSON parsing error")
            return  self._evaluate_against_criteria(text_context)
        
        except Exception as e:
            # Handle all other exceptions (service errors, network issues, etc.)
            logger.error(f"Error in RetrieveAndGenerate: {str(e)}")
            logger.warning("Falling back to direct model invocation")
            return  self._evaluate_against_criteria(text_context)
            
    #Backup option is to invoke model if RAG approach does not work
    def _evaluate_against_criteria(self, text: str) -> Dict:
        """
        Directly evaluate the profile against USCIS EB1A eligibility criteria by querying the model
        Used as fallback if RAG fails
        """
        try:
            start_time = time.time()
            logger.info("Starting direct criteria evaluation for EB1A")
            
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
            
            # Create request body
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 3000,
                "temperature": 0.1,
                "top_k": 250,
                "top_p": 1,
                "messages": [
                {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": eval_prompt.replace("\n\nHuman: ", "").replace("\n\nAssistant:", "")
                    }
                            ]
                }
                            ]
            }

            # Invoke model for evaluation
            bedrock_agent = boto3.client('bedrock-agent-runtime')
            response = bedrock_agent.invoke_model(
                modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body),
            )
            response_body = json.loads(response["body"].read())
            # Extract the response from the Messages API format
            completion = response_body.get("content", [{}])[0].get("text", "").strip()
            evaluation = json.loads(self._extract_json(completion))
            
            total_time = time.time() - start_time
            logger.info(f"Completed direct criteria evaluation in {total_time:.3f} seconds")
            
            # Add visa category to result
            evaluation["visa_category"] = self.visa_category
            
            return evaluation

        except Exception as e:
            logger.error(f"Error evaluating against criteria: {str(e)}")
            raise

    def _extract_text_from_pdf(self, file_content: bytes) -> str:
        """Optimized PDF text extraction with time logging"""
        start_time = time.time()
        logger.info(f"Starting PDF extraction for EB1A visa assessment")
        
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
        """
        Optimized JSON extraction with improved error handling
        """
        logger.debug("Extracting JSON from model response")
        try:
            # First try to find JSON in code blocks
            if "```json" in text:
                json_text = text.split("```json")[1].split("```")[0].strip()
                # Validate it's proper JSON
                json.loads(json_text)
                return json_text
            elif "```" in text:
                json_blocks = [
                    block.strip()
                    for block in text.split("```")
                    if "{" in block and "}" in block
                ]
                if json_blocks:
                    json_text = json_blocks[0]
                    # Validate it's proper JSON
                    json.loads(json_text)
                    return json_text

            # Then try to find the outermost JSON object in the text
            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            if json_start >= 0 and json_end > 0:
                json_text = text[json_start:json_end]
                # Validate it's proper JSON
                json.loads(json_text)
                return json_text
                
            # If we reach here, no JSON was found
            logger.error("No valid JSON found in response")
            raise ValueError("No JSON content found in response")
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            # Try a more aggressive approach for malformed JSON
            if "{" in text and "}" in text:
                try:
                    # Extract everything between the first { and the last }
                    json_text = text[text.find("{"):text.rfind("}")+1]
                    # Clean up common issues like unquoted property names
                    json_text = json_text.replace("'", "\"")
                    # Try parsing again
                    json.loads(json_text)
                    return json_text
                except json.JSONDecodeError:
                    # If still failing, raise the original error
                    pass
            raise ValueError(f"Failed to extract valid JSON: {str(e)}")

    def _retrieve_and_generate_two_step(self, query: str, text_context: str) -> Dict:
        """
        Two-step RAG approach:
        1. Use retrieve to get explicit references from the knowledge base
        2. Use the references to augment the generation with Claude
        """
        start_time = time.time()
        logger.info("Starting two-step RAG for EB1A eligibility assessment")
    
        try:
            # Step 1: Retrieve references from the knowledge base
            bedrock_agent = boto3.client('bedrock-agent-runtime')
            retrieval_response = bedrock_agent.retrieve(
                knowledgeBaseId=self.kb_id,
                retrievalQuery={
                    "text": query
                },
                retrievalConfiguration={
                    "vectorSearchConfiguration": {
                        "numberOfResults": 10,
                        "filter": {
                            "andAll": [
                                {
                                    "equals": {
                                        "key": "petition_type",
                                        "value": "EB1A"
                                    }
                                },
                                {
                                    "equals": {
                                        "key": "petition_status",
                                        "value": "rejected_appealed"
                                    }
                                }
                            ]
                        }
                    }
                }
            )
            
            # Extract and format the retrieved citations
            retrieved_citations = []
            citation_texts = []
            
            for result in retrieval_response.get('retrievalResults', []):
                content = result.get('content', {}).get('text', '')
                location = result.get('location', {})
                score = result.get('score', 0)
                
                citation_info = {
                    "text": content,
                    "source": location.get('type', ''),
                    "source_location": location.get('s3Location', {}).get('uri', '') if location.get('type') == 'S3' else '',
                    "score": score
                }
                retrieved_citations.append(citation_info)
                citation_texts.append(f"Citation {len(citation_texts)+1}:\n{content[:800]}...\n\n")
            
            # Step 2: Use Claude to generate the assessment with the retrieved references
            augmented_context = "\n".join(citation_texts)
            
            # Create prompt for EB1A assessment with the retrieved citations
            generation_prompt = (
                f"You are an immigration expert specializing in EB1A petitions. "
                f"I'll provide you with rejected petition examples from the USCIS knowledge base "
                f"followed by a resume to evaluate.\n\n"
                f"KNOWLEDGE BASE CITATIONS ABOUT REJECTED EB1A PETITIONS:\n"
                f"{augmented_context}\n\n"
                f"TASK: Evaluate this resume against all 10 USCIS criteria for Extraordinary Ability. "
                f"For each criterion, determine if the evidence is Strong, Moderate, Weak, or Not Present. "
                f"Use the rejected petition examples to identify potential vulnerabilities.\n\n"
                f"Resume text:\n{text_context}\n\n"
                f"Return your evaluation in JSON format with the following structure ONLY:\n"
                f'{{\n'
                f'  "criteria_evaluation": {{\n'
                f'    "criterion_name": {{\n'
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
                f'}}'
            )
            
            # Create request body for Claude generation
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 3000,
                "temperature": 0.1,
                "top_k": 250,
                "top_p": 1,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": generation_prompt
                            }
                        ]
                    }
                ]
            }
            
            # Create a bedrock-runtime client for invoking the model
            bedrock_runtime = boto3.client('bedrock-runtime')
            
            # Invoke Claude for generation
            response = bedrock_runtime.invoke_model(
                modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body),
            )
            
            response_body = json.loads(response["body"].read())
            generation = response_body.get("content", [{}])[0].get("text", "").strip()
            
            # Extract and parse JSON from the generation
            json_result = self._extract_json(generation)
            parsed_result = json.loads(json_result)
            
            # Add citation information with source tracking
            parsed_result["citations"] = retrieved_citations
            
            # Add visa category to result
            parsed_result["visa_category"] = self.visa_category
                
            # Log completion time
            total_time = time.time() - start_time
            logger.info(f"Two-step RAG completed in {total_time:.3f} seconds")
            
            return parsed_result
        
        except (json.JSONDecodeError, ValueError) as e:
            # Handle issues with JSON parsing or extraction
            logger.error(f"Error parsing response from two-step RAG: {str(e)}")
            logger.warning("Falling back to direct model invocation due to JSON parsing error")
            return self._evaluate_against_criteria(text_context)
        
        except Exception as e:
            # Handle all other exceptions (service errors, network issues, etc.)
            logger.error(f"Error in two-step RAG: {str(e)}")
            logger.warning("Falling back to direct model invocation")
            return self._evaluate_against_criteria(text_context)
        
    @tracer.capture_method
    def process_document(self, file_content: bytes) -> Dict:
        """Process document with RAG-enhanced criteria evaluation"""
        overall_start = time.time()
        
        try:
            # Extract text from PDF
            pdf_start = time.time()
            text = self._extract_text_from_pdf(file_content)
            pdf_time = time.time() - pdf_start
            logger.info(f"Extracted PDF text in {pdf_time:.3f} seconds")
            
            
            # First try RAG-based evaluation
            rag_start = time.time()
            logger.info("Attempting RAG-enhanced evaluation")
            
            try:
                # Create specific query for knowledge base context
                kb_query = "Latest USCIS guidelines and precedent decisions for EB1A visa criteria, specifically examples of rejected petitions and appeals"
                
                # Perform RAG evaluation with the retrieve_and_generate method
                criteria_analysis =  self._retrieve_and_generate_two_step(kb_query, text)
                evaluation_method = "RAG"
                rag_time = time.time() - rag_start
                logger.info(f"Completed RAG-enhanced evaluation in {rag_time:.3f} seconds")
                
            except Exception as e:
                logger.error(f"RAG evaluation failed: {str(e)}")
                logger.info("Falling back to direct model evaluation")
                
                # Fall back to direct model evaluation
                direct_start = time.time()
                criteria_analysis =  self._evaluate_against_criteria(text)
                evaluation_method = "direct"
                rag_time = time.time() - rag_start
                direct_time = time.time() - direct_start
                logger.info(f"Completed direct model evaluation in {direct_time:.3f} seconds")
            
            # Calculate success probability based on criteria evaluation
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
            
            # Total processing time
            total_time = time.time() - overall_start
            logger.info(f"Complete document processing finished in {total_time:.3f} seconds")
            
            # Include timing data in the result
            timing_data = {
                "pdf_extraction_time": round(pdf_time, 3),
                "evaluation_time": round(time.time() - rag_start, 3),
                "evaluation_method": evaluation_method,
                "total_processing_time": round(total_time, 3),
            }

            return {
                "visa_category": "EB1A",
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
        result =  processor.process_document(file_content)
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