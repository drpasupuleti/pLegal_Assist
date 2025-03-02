import json
import os
import time
from io import BytesIO
from typing import Dict, List, Any

# Import PDF processing libraries
import PyPDF2

# AWS libraries
import boto3
from aws_lambda_powertools import Logger, Tracer

# Import local modules
from resume_analyzer import ResumeAnalyzer
from kb_retriever import KnowledgeBaseRetriever

logger = Logger()
tracer = Tracer()

class DocumentProcessor:
    VALID_VISA_CATEGORIES = {"EB1A"}
    
    def __init__(self, visa_category: str = "EB1A"):
        if visa_category not in self.VALID_VISA_CATEGORIES:
            raise ValueError(f"Invalid visa category. Only EB1A is supported.")
        
        self.visa_category = "EB1A"  # Hardcode to EB1A as we're only supporting this
        self.kb_id = os.environ.get("KNOWLEDGE_BASE_ID", "BYASZZZFRM")
        self.resume_analyzer = ResumeAnalyzer()
        self.kb_retriever = KnowledgeBaseRetriever(self.kb_id)
        logger.info(f"Initialized DocumentProcessor for EB1A with KB ID: {self.kb_id}")

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

    def _evaluate_with_llm(self, resume_text: str, structured_profile: Dict[str, Any], kb_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate the resume against EB1A criteria using the LLM with structured profile and KB results
        """
        start_time = time.time()
        logger.info("Starting LLM evaluation for EB1A eligibility")
        
        try:
            # Extract top citations from KB results
            citations = kb_results.get("consolidated_citations", [])
            citation_texts = []
            
            for i, citation in enumerate(citations[:7]):  # Limit to top 7 citations
                citation_text = citation.get("text", "")
                query_type = citation.get("query_type", "")
                
                # Format citation with context about which query generated it
                citation_texts.append(f"CITATION {i+1} [{query_type}]:\n{citation_text[:800]}...\n")
            
            # Compile relevant criteria from structured profile
            relevant_criteria = structured_profile.get("relevant_criteria", [])
            criteria_evidence = structured_profile.get("criteria_evidence", {})
            
            # Construct a summary of the structured profile
            profile_summary = []
            for criterion_key in relevant_criteria:
                criterion_info = criteria_evidence.get(criterion_key, {})
                criterion_name = criterion_info.get("name", "")
                evidence_count = criterion_info.get("evidence_count", 0)
                evidence = criterion_info.get("evidence", [])
                
                # Format evidence summary
                profile_summary.append(f"CRITERION: {criterion_name}")
                profile_summary.append(f"EVIDENCE COUNT: {evidence_count}")
                profile_summary.append("SAMPLE EVIDENCE:")
                
                # Include up to 3 pieces of evidence per criterion
                for i, evidence_item in enumerate(evidence[:3]):
                    profile_summary.append(f"- {evidence_item}")
                
                profile_summary.append("")
            
            # Generate comprehensive prompt for LLM evaluation
            prompt = (
                f"You are an immigration expert specializing in EB1A visa petitions. "
                f"I will provide you with three key components for evaluation:\n"
                f"1. A structured analysis of a resume highlighting potential EB1A criteria\n"
                f"2. Relevant examples of rejected EB1A petitions from USCIS knowledge base\n"
                f"3. The full resume text\n\n"
                
                f"STRUCTURED PROFILE SUMMARY:\n"
                f"This resume appears to have evidence for these EB1A criteria:\n"
                f"{''.join(profile_summary)}\n\n"
                
                f"KNOWLEDGE BASE CITATIONS FROM REJECTED PETITIONS:\n"
                f"{''.join(citation_texts)}\n\n"
                
                f"TASK: Evaluate this resume against all 10 USCIS criteria for Extraordinary Ability. "
                f"For each criterion, determine if the evidence is Strong, Moderate, Weak, or Not Present. "
                f"Pay special attention to the criteria identified in the structured profile, "
                f"but evaluate all criteria thoroughly. Use the rejected petition examples to identify common pitfalls.\n\n"
                
                f"FULL RESUME TEXT:\n{resume_text[:10000]}...\n\n"  # Truncate if extremely long
                
                f"Return your evaluation in JSON format with the following structure ONLY:\n"
                f'{{\n'
                f'  "criteria_evaluation": {{\n'
                f'    "criterion_name": {{\n'
                f'      "evidence_level": "Strong|Moderate|Weak|Not Present",\n'
                f'      "evidence_found": "Description of evidence found",\n'
                f'      "likely_approved": true|false,\n'
                f'      "comparison_to_rejected_cases": "How this evidence compares to similar rejected cases"\n'
                f'    }},\n'
                f'    // repeat for all 10 criteria\n'
                f'  }},\n'
                f'  "met_criteria_count": 0-10,\n'
                f'  "threshold_met": true|false,\n'
                f'  "strongest_criteria": ["criterion1", "criterion2"],\n'
                f'  "weakest_criteria": ["criterion1", "criterion2"],\n'
                f'  "suggested_improvements": ["improvement1", "improvement2"],\n'
                f'  "overall_assessment": "Brief overall assessment of the application"\n'
                f'}}'
            )
            
            # Create request body for Claude generation
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4000,
                "temperature": 0.1,
                "top_k": 250,
                "top_p": 1,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
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
            
            # Add metadata about the evaluation process
            parsed_result["visa_category"] = self.visa_category
            parsed_result["evaluation_metadata"] = {
                "profile_criteria_count": len(relevant_criteria),
                "citation_count": len(citations),
                "evaluation_time": round(time.time() - start_time, 3)
            }
            
            # Log completion time
            total_time = time.time() - start_time
            logger.info(f"LLM evaluation completed in {total_time:.3f} seconds")
            
            return parsed_result
            
        except Exception as e:
            logger.error(f"Error in LLM evaluation: {str(e)}")
            # Fall back to direct model evaluation if structured approach fails
            logger.warning("Falling back to direct model evaluation")
            return self._evaluate_against_criteria(resume_text)
    
    def _evaluate_against_criteria(self, text: str) -> Dict[str, Any]:
        """
        Directly evaluate the profile against USCIS EB1A eligibility criteria by querying the model
        Used as fallback if structured approach fails
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
                f"As an immigration expert specializing in EB1A petitions, "
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
                f'}}'
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
                                "text": eval_prompt
                            }
                        ]
                    }
                ]
            }

            # Invoke model for evaluation
            bedrock_runtime = boto3.client('bedrock-runtime')
            response = bedrock_runtime.invoke_model(
                modelId="anthropic.claude-3-5-sonnet-v2",
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body),
            )
            
            response_body = json.loads(response["body"].read())
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

    @tracer.capture_method
    def process_document(self, file_content: bytes) -> Dict[str, Any]:
        """Process document with structured 3-stage approach"""
        overall_start = time.time()
        
        try:
            # STAGE 1: Extract text from PDF and analyze resume
            pdf_start = time.time()
            resume_text = self._extract_text_from_pdf(file_content)
            pdf_time = time.time() - pdf_start
            logger.info(f"Extracted PDF text in {pdf_time:.3f} seconds")
            
            # Analyze resume to extract structured profile
            analysis_start = time.time()
            structured_profile = self.resume_analyzer.analyze_resume(resume_text)
            analysis_time = time.time() - analysis_start
            logger.info(f"Completed resume analysis in {analysis_time:.3f} seconds")
            
            # STAGE 2: Generate targeted queries and retrieve from knowledge base
            retrieval_start = time.time()
            
            # Generate queries based on the structured profile
            queries = self.resume_analyzer.generate_queries(structured_profile)
            
            # Perform targeted retrievals for all queries
            kb_results = self.kb_retriever.perform_targeted_retrievals(queries)
            retrieval_time = time.time() - retrieval_start
            logger.info(f"Completed knowledge base retrievals in {retrieval_time:.3f} seconds")
            
            # STAGE 3: Evaluate with LLM using structured context
            evaluation_start = time.time()
            criteria_analysis = self._evaluate_with_llm(resume_text, structured_profile, kb_results)
            evaluation_time = time.time() - evaluation_start
            logger.info(f"Completed LLM evaluation in {evaluation_time:.3f} seconds")
            
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
                "resume_analysis_time": round(analysis_time, 3),
                "knowledge_base_retrieval_time": round(retrieval_time, 3),
                "llm_evaluation_time": round(evaluation_time, 3),
                "total_processing_time": round(total_time, 3),
            }

            return {
                "visa_category": "EB1A",
                "structured_profile": {
                    "relevant_criteria": structured_profile.get("relevant_criteria", []),
                    "criteria_count": len(structured_profile.get("relevant_criteria", []))
                },
                "criteria_analysis": criteria_analysis,
                "success_probability": {
                    "rating": probability,
                    "explanation": explanation
                },
                "citation_count": kb_results.get("citation_count", 0),
                "timing_data": timing_data,
            }

        except Exception as e:
            # Log error with timing information
            total_time = time.time() - overall_start
            logger.error(f"Error processing document after {total_time:.3f} seconds: {str(e)}")
            raise