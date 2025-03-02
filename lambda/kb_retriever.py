import json
import time
from typing import Dict, List, Any

import boto3
from aws_lambda_powertools import Logger

logger = Logger()

class KnowledgeBaseRetriever:
    """
    Handles knowledge base retrieval operations using Bedrock Agent APIs
    Performs targeted retrievals and consolidates results
    """
    
    def __init__(self, kb_id: str):
        self.kb_id = kb_id
        self.bedrock_agent = boto3.client('bedrock-agent-runtime')
        self.logger = logger
        
    def retrieve(self, query: str, filter_conditions: List[Dict[str, Any]] = None, num_results: int = 5) -> Dict[str, Any]:
        """
        Retrieve information from the knowledge base using the provided query
        Allows for filtering based on provided conditions
        """
        start_time = time.time()
        self.logger.info(f"Starting retrieval for query: {query}")
        
        try:
            # Build retrieval configuration
            retrieval_config = {
                "vectorSearchConfiguration": {
                    "numberOfResults": num_results
                }
            }
            
            # Add filters if provided
            if filter_conditions:
                filters = {"andAll": filter_conditions}
                retrieval_config["vectorSearchConfiguration"]["filter"] = filters
                
            # Execute the retrieve call
            response = self.bedrock_agent.retrieve(
                knowledgeBaseId=self.kb_id,
                retrievalQuery={
                    "text": query
                },
                retrievalConfiguration=retrieval_config
            )
            
            # Format results
            retrieved_results = []
            
            for result in response.get('retrievalResults', []):
                content = result.get('content', {}).get('text', '')
                location = result.get('location', {})
                score = result.get('score', 0)
                
                # Create structured result
                result_info = {
                    "text": content,
                    "source": location.get('type', ''),
                    "source_location": location.get('s3Location', {}).get('uri', '') if location.get('type') == 'S3' else '',
                    "score": score
                }
                retrieved_results.append(result_info)
            
            # Calculate retrieval time
            total_time = time.time() - start_time
            self.logger.info(f"Retrieved {len(retrieved_results)} results in {total_time:.3f} seconds")
            
            return {
                "query": query,
                "citation_count": len(retrieved_results),
                "citations": retrieved_results,
                "retrieval_time": round(total_time, 3)
            }
        
        except Exception as e:
            self.logger.error(f"Error in KB retrieval: {str(e)}")
            total_time = time.time() - start_time
            self.logger.error(f"Retrieval operation failed after {total_time:.3f} seconds")
            
            return {
                "query": query,
                "error": str(e),
                "citation_count": 0,
                "citations": [],
                "retrieval_time": round(total_time, 3)
            }
    
    def perform_targeted_retrievals(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute multiple targeted queries and consolidate results
        """
        start_time = time.time()
        self.logger.info(f"Starting targeted retrievals for {len(queries)} queries")
        
        # Default filter for EB1A rejected petitions
        base_filters = [
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
        
        all_results = {}
        consolidated_citations = []
        
        # Track unique citations to avoid duplicates
        seen_citations = set()
        
        for query_info in queries:
            query_text = query_info["query_text"]
            query_type = query_info.get("query_type", "general")
            
            # Determine number of results based on query type
            num_results = 3 if query_type == "general" else 5
            
            # Get results for this query
            self.logger.info(f"Executing {query_type} query: {query_text}")
            results = self.retrieve(query_text, base_filters, num_results)
            
            # Store results by query type
            all_results[query_type] = results
            
            # Add non-duplicate citations to consolidated list
            for citation in results.get("citations", []):
                # Create a simple hash of the content to check for duplicates
                citation_hash = hash(citation.get("text", "")[:200])
                
                if citation_hash not in seen_citations:
                    seen_citations.add(citation_hash)
                    # Add query info to the citation
                    citation["query_source"] = query_text
                    citation["query_type"] = query_type
                    consolidated_citations.append(citation)
        
        # Sort consolidated citations by score (descending)
        consolidated_citations = sorted(
            consolidated_citations, 
            key=lambda x: x.get("score", 0), 
            reverse=True
        )
        
        # Calculate total time
        total_time = time.time() - start_time
        self.logger.info(f"Completed all retrievals in {total_time:.3f} seconds")
        self.logger.info(f"Found {len(consolidated_citations)} unique citations across all queries")
        
        return {
            "query_count": len(queries),
            "retrieval_time": round(total_time, 3),
            "query_results": all_results,
            "consolidated_citations": consolidated_citations[:10]  # Limit to top 10 citations
        }