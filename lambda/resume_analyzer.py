import json
import re
import time
from typing import Dict, List, Set, Any

from aws_lambda_powertools import Logger

logger = Logger()

class ResumeAnalyzer:
    """
    Analyzes resume text to extract information relevant to EB1A criteria
    Uses a structured extraction approach to identify evidence for each criterion
    """
    
    # Define EB1A criteria and corresponding regex patterns
    EB1A_CRITERIA = {
        "awards": {
            "name": "nationally or internationally recognized prizes or awards for excellence",
            "patterns": [
                r"(?i)award(?:ed|s)?|prize|medal|honor|recognition|distinction",
                r"(?i)recipient of|won|received|granted",
                r"(?i)fellowship|scholarship"
            ]
        },
        "membership": {
            "name": "membership in associations that require outstanding achievement",
            "patterns": [
                r"(?i)member(?:ship)? (?:of|in)|fellow of|elected to|inducted into",
                r"(?i)professional societ(?:y|ies)|association|organization|board|committee",
                r"(?i)reviewer for|editorial board|review committee"
            ]
        },
        "published_material": {
            "name": "published material about the alien in professional publications",
            "patterns": [
                r"(?i)featured in|profiled in|interviewed by|article about|story on",
                r"(?i)media coverage|press release|news article|spotlight",
                r"(?i)biography|profile in"
            ]
        },
        "judging": {
            "name": "judging the work of others in the field",
            "patterns": [
                r"(?i)judge|jury|reviewer|referee|evaluator|examiner",
                r"(?i)review(?:ed|ing)|evaluat(?:ed|ing)|assess(?:ed|ing)",
                r"(?i)selection committee|panel|board"
            ]
        },
        "original_contributions": {
            "name": "original scientific, scholarly, or business-related contributions of major significance",
            "patterns": [
                r"(?i)innovat(?:ed|ion|ive)|pioneer(?:ed|ing)|breakthrough|groundbreaking",
                r"(?i)patent(?:ed|s)?|invention|discover(?:y|ed)|develop(?:ed|ment)",
                r"(?i)first to|novel|revolutionary|transformative|significant contribution"
            ]
        },
        "authorship": {
            "name": "authorship of scholarly articles in professional journals or major media",
            "patterns": [
                r"(?i)author(?:ed)?|publish(?:ed)?|wrote|paper|article|chapter|publication",
                r"(?i)journal|proceedings|conference|symposium|book",
                r"(?i)co-author(?:ed)?|contributor|manuscript|research paper"
            ]
        },
        "exhibitions": {
            "name": "display of work at artistic exhibitions or showcases",
            "patterns": [
                r"(?i)exhibit(?:ed|ion)?|showcase(?:d)?|display(?:ed)?|presented at|showing",
                r"(?i)gallery|museum|venue|exhibition hall|show",
                r"(?i)installation|performance|presentation|demonstration"
            ]
        },
        "leading_role": {
            "name": "performing a leading or critical role for distinguished organizations",
            "patterns": [
                r"(?i)lead(?:er|ing)?|director|manager|supervisor|head|chief|chair(?:person)?",
                r"(?i)found(?:er|ed)|executive|president|CEO|CTO|CFO|COO|VP|vice president",
                r"(?i)principal|coordinator|administrator|officer"
            ]
        },
        "high_salary": {
            "name": "high salary or remuneration compared to others in the field",
            "patterns": [
                r"(?i)salary|compensation|remuneration|wage|income|earning|pay",
                r"(?i)\$\s*\d[\d,.]*|\d[\d,.]* USD|annual|compensation package",
                r"(?i)bonus|stock options|equity|benefits package"
            ]
        },
        "commercial_success": {
            "name": "commercial success in the performing arts",
            "patterns": [
                r"(?i)box office|ticket sales|attendance|audience|sold out",
                r"(?i)bestseller|top.selling|commercial success|profitable|revenue",
                r"(?i)gross(?:ed|ing)|sales|earnings|proceeds|profit"
            ]
        }
    }
    
    def __init__(self):
        self.logger = logger
    
    def _extract_evidence_for_criterion(self, text: str, criterion: str, patterns: List[str]) -> List[str]:
        """
        Extract evidence for a specific criterion based on regex patterns
        Returns a list of sentences that contain matches
        """
        evidence = []
        
        # Split text into sentences (simple split by period, exclamation, question mark)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        # Combine patterns into a single regex
        combined_pattern = '|'.join(patterns)
        
        for sentence in sentences:
            if re.search(combined_pattern, sentence, re.IGNORECASE):
                # Clean up the sentence
                clean_sentence = sentence.strip()
                if clean_sentence and len(clean_sentence) > 10:  # Ignore very short matches
                    evidence.append(clean_sentence)
        
        return evidence
    
    def analyze_resume(self, text: str) -> Dict[str, Any]:
        """
        Analyze resume text and extract structured profile for EB1A criteria
        """
        start_time = time.time()
        self.logger.info("Starting resume analysis for EB1A structured profile extraction")
        
        # Initialize structured profile
        structured_profile = {
            "criteria_evidence": {},
            "relevant_criteria": [],
            "resume_length": len(text),
            "resume_word_count": len(text.split())
        }
        
        # Extract evidence for each criterion
        for criterion_key, criterion_info in self.EB1A_CRITERIA.items():
            criterion_name = criterion_info["name"]
            patterns = criterion_info["patterns"]
            
            evidence = self._extract_evidence_for_criterion(text, criterion_name, patterns)
            
            # Add to profile if evidence found
            if evidence:
                structured_profile["criteria_evidence"][criterion_key] = {
                    "name": criterion_name,
                    "evidence": evidence,
                    "evidence_count": len(evidence)
                }
                
                # Consider it relevant if we find significant evidence
                if len(evidence) >= 2:
                    structured_profile["relevant_criteria"].append(criterion_key)
        
        # Calculate analysis time
        analysis_time = time.time() - start_time
        structured_profile["analysis_time"] = round(analysis_time, 3)
        
        self.logger.info(f"Resume analysis completed in {analysis_time:.3f} seconds")
        self.logger.info(f"Found evidence for {len(structured_profile['criteria_evidence'])} criteria")
        self.logger.info(f"Identified {len(structured_profile['relevant_criteria'])} relevant criteria")
        
        return structured_profile
    
    def generate_queries(self, structured_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate knowledge base queries based on the structured profile
        Focus on relevant criteria to get targeted information
        """
        queries = []
        
        # Generate a general query for EB1A guidelines
        queries.append({
            "query_text": "USCIS guidelines and requirements for all EB1A criteria and evidence standards",
            "query_type": "general"
        })
        
        # Generate targeted queries for each relevant criterion
        for criterion_key in structured_profile.get("relevant_criteria", []):
            criterion_info = structured_profile["criteria_evidence"].get(criterion_key, {})
            criterion_name = criterion_info.get("name", "")
            
            if criterion_name:
                query = {
                    "query_text": f"Rejected EB1A petitions where the evidence for '{criterion_name}' was deemed insufficient or inadequate",
                    "query_type": "criterion_specific",
                    "criterion": criterion_key
                }
                queries.append(query)
        
        # Add a query for threshold determination cases
        if len(structured_profile.get("relevant_criteria", [])) >= 2:
            queries.append({
                "query_text": "Rejected EB1A petitions where the applicant met 2 or 3 criteria but was still denied",
                "query_type": "threshold"
            })
        
        logger.info(f"Generated {len(queries)} targeted knowledge base queries")
        return queries