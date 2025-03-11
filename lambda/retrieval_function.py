import json
import os
import boto3
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
USCIS_URL = 'https://www.uscis.gov/administrative-appeals/aao-decisions'
VISA_CATEGORIES = ['EB-1', 'EB-2']
DECISION_TYPE = 'Denial'

# Initialize S3 client
s3 = boto3.client('s3')
RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME')

def get_session():
    """Create a requests session with appropriate headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml',
        'Accept-Language': 'en-US,en;q=0.9'
    })
    return session

def get_processed_document_ids():
    """Retrieve already processed document IDs from S3"""
    processed_ids = set()
    
    try:
        response = s3.get_object(
            Bucket=RAW_BUCKET_NAME,
            Key='processed-docs-registry.json'
        )
        registry = json.loads(response['Body'].read().decode('utf-8'))
        processed_ids = set(registry.get('ids', []))
        logger.info(f"Loaded {len(processed_ids)} previously processed document IDs")
    except Exception as e:
        logger.info(f"No existing document registry found: {str(e)}")
    
    return processed_ids

def update_processed_document_ids(processed_ids):
    """Update the registry of processed document IDs in S3"""
    try:
        s3.put_object(
            Bucket=RAW_BUCKET_NAME,
            Key='processed-docs-registry.json',
            Body=json.dumps({
                'lastUpdate': datetime.now().isoformat(),
                'ids': list(processed_ids)
            }),
            ContentType='application/json'
        )
        logger.info(f"Updated document registry with {len(processed_ids)} document IDs")
    except Exception as e:
        logger.error(f"Error updating document registry: {str(e)}")

def handler(event, context):
    """Main Lambda handler function"""
    logger.info("Starting document retrieval process")
    
    session = get_session()
    downloaded_documents = []
    processed_ids = get_processed_document_ids()
    
    try:
        # Get the main page to parse the search form
        response = session.get(USCIS_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract form action URL and parameters (adjust based on actual site)
        form = soup.find('form', {'id': 'search-form'})
        if not form:
            logger.error("Could not find search form on page")
            return {
                'statusCode': 500,
                'body': {'message': 'Could not find search form'}
            }
        
        form_action = urljoin(USCIS_URL, form.get('action', ''))
        form_method = form.get('method', 'get').lower()
        
        # Process each visa category
        for visa_category in VISA_CATEGORIES:
            logger.info(f"Processing visa category: {visa_category}")
            
            # Prepare search form data
            form_data = {
                'visa-category': visa_category,
                'decision-type': DECISION_TYPE,
                'search': 'true'
            }
            
            # Submit search
            if form_method == 'post':
                search_response = session.post(form_action, data=form_data)
            else:
                search_response = session.get(form_action, params=form_data)
            
            # Parse search results
            results_soup = BeautifulSoup(search_response.text, 'html.parser')
            document_links = results_soup.select('.search-results .document-link')
            
            logger.info(f"Found {len(document_links)} document links for {visa_category}")
            
            # Process each document
            for link in document_links:
                try:
                    doc_url = urljoin(USCIS_URL, link.get('href'))
                    doc_id = link.get('data-document-id') or doc_url.split('/')[-1]
                    doc_title = link.text.strip()
                    
                    # Skip if already processed
                    if doc_id in processed_ids:
                        logger.info(f"Skipping already processed document: {doc_id}")
                        continue
                    
                    logger.info(f"Processing document: {doc_title} ({doc_id})")
                    
                    # Get document page
                    doc_response = session.get(doc_url)
                    doc_soup = BeautifulSoup(doc_response.text, 'html.parser')
                    
                    # Find PDF download link
                    pdf_link = doc_soup.select_one('a.document-download')
                    if not pdf_link:
                        logger.warning(f"No PDF download link found for {doc_id}")
                        continue
                    
                    pdf_url = urljoin(USCIS_URL, pdf_link.get('href'))
                    
                    # Download PDF
                    pdf_response = session.get(pdf_url)
                    
                    # Extract any additional metadata from the document page
                    metadata = {
                        'id': doc_id,
                        'title': doc_title,
                        'sourceUrl': doc_url,
                        'visaCategory': visa_category,
                        'documentType': DECISION_TYPE,
                        'retrievalDate': datetime.now().isoformat()
                    }
                    
                    # Try to extract decision date if available
                    date_element = doc_soup.select_one('.document-date')
                    if date_element:
                        metadata['decisionDate'] = date_element.text.strip()
                    
                    # Generate S3 key with organized structure
                    s3_key = f"{visa_category}/{DECISION_TYPE}/{doc_id}.pdf"
                    
                    # Upload to S3
                    s3.put_object(
                        Bucket=RAW_BUCKET_NAME,
                        Key=s3_key,
                        Body=pdf_response.content,
                        ContentType='application/pdf',
                        Metadata={k: str(v) for k, v in metadata.items()}
                    )
                    
                    # Add to processed set and downloaded list
                    processed_ids.add(doc_id)
                    downloaded_documents.append({
                        'id': doc_id,
                        's3Key': s3_key,
                        'metadata': metadata
                    })
                    
                    logger.info(f"Successfully downloaded and uploaded document: {doc_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing document {doc_id if 'doc_id' in locals() else 'unknown'}: {str(e)}")
        
        # Update the registry of processed documents
        update_processed_document_ids(processed_ids)
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Document retrieval completed successfully',
                'documentsProcessed': len(downloaded_documents),
                'documents': downloaded_documents
            }
        }
    
    except Exception as e:
        logger.error(f"Error during document retrieval: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': {
                'message': 'Error during document retrieval',
                'error': str(e)
            }
        }