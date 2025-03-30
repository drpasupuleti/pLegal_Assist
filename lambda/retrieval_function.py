import json
import os
import boto3
import logging
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urljoin, parse_qs, urlparse

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
USCIS_BASE_URL = 'https://www.uscis.gov'
AAO_DECISIONS_URL = 'https://www.uscis.gov/administrative-appeals/aao-decisions/aao-non-precedent-decisions'

# Define petition type mappings - only EB1A for now
PETITION_TYPES = {
    'EB-1A': 19,  # uri_1=19 for EB1A
    # 'EB-2': 20,   # EB2NIW - commented out until specifically requested
}

# Month name to number and vice versa
MONTH_NAMES = {
    'Jan': 1, 'January': 1,
    'Feb': 2, 'February': 2,
    'Mar': 3, 'March': 3,
    'Apr': 4, 'April': 4,
    'May': 5, 'May': 5,  # Both entries for consistency
    'Jun': 6, 'June': 6,
    'Jul': 7, 'July': 7,
    'Aug': 8, 'August': 8,
    'Sep': 9, 'September': 9, 
    'Oct': 10, 'October': 10,
    'Nov': 11, 'November': 11,
    'Dec': 12, 'December': 12
}

MONTH_NUMBERS = {v: k for k, v in MONTH_NAMES.items() if len(k) > 3}  # Only full names

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

def check_file_exists_in_s3(bucket, key):
    """Check if a file already exists in S3"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        # If we get an error (typically 404), the file doesn't exist
        return False

def get_last_month():
    """Get the month number and year for last month"""
    today = datetime.now()
    last_month = today.replace(day=1) - timedelta(days=1)
    return {
        'month': last_month.month,
        'month_name': MONTH_NUMBERS[last_month.month],
        'year': last_month.year
    }

def extract_date_from_filename(filename):
    """
    Extract date from filenames that follow the USCIS pattern like:
    - JAN302025_05B2203.pdf (Month, day, year followed by an underscore and ID)
    
    Returns a datetime object if successful, None otherwise
    """
    # Match pattern for JAN302025_05B2203.pdf
    pattern = r'([A-Za-z]{3})(\d{1,2})(\d{4})_'
    match = re.search(pattern, filename)
    
    if match:
        month_str, day_str, year_str = match.groups()
        try:
            month = MONTH_NAMES.get(month_str.capitalize(), None)
            if month:
                day = int(day_str)
                year = int(year_str)
                return datetime(year, month, day)
        except (ValueError, TypeError):
            logger.error(f"Error parsing date from filename: {filename}")
            pass
    
    # If no match found with primary pattern, look for any 3-letter month name
    for month_name in MONTH_NAMES.keys():
        if len(month_name) == 3 and month_name.upper() in filename.upper():
            logger.info(f"Found month {month_name} in filename {filename}, attempting to extract date")
            # Try to find digits near the month name
            parts = re.split(r'[_\-\s.]', filename)
            for part in parts:
                if month_name.upper() in part.upper() and any(c.isdigit() for c in part):
                    logger.info(f"Analyzing part with month and numbers: {part}")
                    # Extract numbers in the vicinity of the month name
                    digits = ''.join(c for c in part if c.isdigit())
                    if len(digits) >= 6:  # At least day (2) + year (4)
                        logger.info(f"Found potential date digits: {digits}")
    
    logger.warning(f"Could not extract date from filename: {filename}")
    return None

def is_from_last_month(filename):
    """Check if a document is from last month based on its filename"""
    last_month_info = get_last_month()
    file_date = extract_date_from_filename(filename)
    
    if file_date:
        # Check if the document is EXACTLY from last month and year (not older)
        is_last_month = (file_date.month == last_month_info['month'] and 
                          file_date.year == last_month_info['year'])
        
        if is_last_month:
            logger.info(f"Document date matches exactly last month: {file_date.month}/{file_date.year}")
            return True
        else:
            logger.info(f"Document date {file_date.month}/{file_date.year} does not match last month {last_month_info['month']}/{last_month_info['year']}")
            return False
    
    logger.warning(f"Could not determine date from filename: {filename}")
    return False  # If we can't determine the date, we'll be cautious and skip it

def handler(event, context):
    """Main Lambda handler function"""
    logger.info("Starting document retrieval process")
    
    session = get_session()
    downloaded_documents = []
    processed_ids = get_processed_document_ids()
    
    # Get last month info
    last_month_info = get_last_month()
    logger.info(f"Fetching documents for month: {last_month_info['month_name']} {last_month_info['year']}")
    
    try:
        for visa_category, petition_id in PETITION_TYPES.items():
            logger.info(f"Processing visa category: {visa_category} (ID: {petition_id})")
            
            # Prepare query parameters - use 'All' for month and add items_per_page
            query_params = {
                'uri_1': petition_id,      # Petition type (EB1A, EB2NIW)
                'm': 'All',                # Get all months - we'll filter later
                'y': 'All',                # All years
                'items_per_page': 100      # Get more results per page
            }
            
            # Make request with query parameters
            search_url = f"{AAO_DECISIONS_URL}"
            logger.info(f"Searching URL: {search_url} with params: {query_params}")
            search_response = session.get(search_url, params=query_params)
            
            if search_response.status_code != 200:
                logger.error(f"Error accessing search page: {search_response.status_code}")
                continue
                
            # Parse search results page
            results_soup = BeautifulSoup(search_response.text, 'html.parser')
            
            # Try multiple selectors to find document links
            document_links = []
            
            # Try all possible selectors
            selectors = [
                '.usa-card-group .usa-card__container a',
                '.views-row a',
                'a[href*="/administrative-appeals/"]',
                'table a[href$=".pdf"]',
                'a[href$=".pdf"]'  # Last resort - all PDF links
            ]
            
            for selector in selectors:
                document_links = results_soup.select(selector)
                logger.info(f"Selector '{selector}' found {len(document_links)} links")
                if len(document_links) > 0:
                    break
            
            # Process each document
            processed_files_count = 0
            encountered_older_document = False
            older_document_count = 0
            
            for link in document_links:
                try:
                    # If we've already encountered documents from before the previous month
                    # and have processed at least one document, we can stop (assuming documents
                    # are listed in reverse chronological order)
                    if encountered_older_document and processed_files_count > 0:
                        logger.info("Already found and processed previous month documents, and encountered older documents. Stopping processing.")
                        break
                    
                    # If we've checked several documents (at least 10) and all are older than previous month,
                    # it's likely there are no documents from previous month
                    if encountered_older_document and older_document_count >= 10 and processed_files_count == 0:
                        logger.warning(f"Checked {older_document_count} documents, all older than previous month. No documents found for previous month. Stopping processing.")
                        break
                    
                    doc_url = urljoin(USCIS_BASE_URL, link.get('href'))
                    
                    # Skip if not a PDF link
                    if not doc_url.endswith('.pdf'):
                        logger.debug(f"Skipping non-PDF link: {doc_url}")
                        continue
                    
                    # Extract document ID and filename from URL
                    filename = doc_url.split('/')[-1]
                    doc_id = filename.replace('.pdf', '')
                    
                    # Log filename for debugging
                    logger.info(f"Checking file: {filename}")
                    
                    # Check if this document is from last month based on filename
                    if not is_from_last_month(filename):
                        logger.info(f"Skipping document not from previous month: {filename}")
                        
                        # Check if this document is older than the previous month
                        file_date = extract_date_from_filename(filename)
                        last_month_info = get_last_month()
                        if file_date:
                            # If we find a document from before the previous month, flag it
                            if (file_date.year < last_month_info['year'] or 
                                (file_date.year == last_month_info['year'] and file_date.month < last_month_info['month'])):
                                logger.info(f"Document from {file_date.month}/{file_date.year} is older than previous month {last_month_info['month']}/{last_month_info['year']}. Marking to stop processing.")
                                encountered_older_document = True
                                older_document_count += 1
                        
                        continue
                    
                    # Log the matched document from last month
                    logger.info(f"Found document from previous month: {filename}")
                    
                    doc_title = link.text.strip() if link.text else f"Document {doc_id}"
                    
                    # Skip if already processed
                    if doc_id in processed_ids:
                        logger.info(f"Skipping already processed document: {doc_id}")
                        continue
                    
                    logger.info(f"Processing document: {doc_title} ({doc_id})")
                    
                    # Extract date from filename for metadata
                    file_date = extract_date_from_filename(filename)
                    month_name = last_month_info['month_name']
                    month_num = last_month_info['month']
                    
                    if file_date:
                        month_name = MONTH_NUMBERS.get(file_date.month, 'Unknown')
                        month_num = file_date.month
                    
                    metadata = {
                        'id': doc_id,
                        'title': doc_title,
                        'sourceUrl': doc_url,
                        'visaCategory': visa_category,
                        'document_month': month_name,
                        'document_month_num': str(month_num),
                        'document_year': str(last_month_info['year']),
                        'retrievalDate': datetime.now().isoformat()
                    }
                    
                    # Generate S3 key with organized structure
                    s3_key = f"{visa_category}/{month_name}/{doc_id}.pdf"
                    
                    # Check if file already exists in S3
                    if check_file_exists_in_s3(RAW_BUCKET_NAME, s3_key):
                        logger.info(f"File already exists in S3: {s3_key}")
                        
                        # Add to processed IDs so we don't try to download it again
                        processed_ids.add(doc_id)
                        downloaded_documents.append({
                            'id': doc_id,
                            's3Key': s3_key,
                            'metadata': metadata,
                            'status': 'already_exists'
                        })
                        continue
                    
                    # Download PDF directly
                    pdf_response = session.get(doc_url)
                    
                    if pdf_response.status_code != 200:
                        logger.error(f"Failed to download PDF {doc_id}: {pdf_response.status_code}")
                        continue
                    
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
                    
                    # Update processed count
                    processed_files_count += 1
                    logger.info(f"Successfully downloaded document {processed_files_count} for {visa_category}: {doc_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing document {doc_id if 'doc_id' in locals() else 'unknown'}: {str(e)}")
            
            logger.info(f"Completed processing {visa_category} - downloaded {processed_files_count} documents from previous month")
        
        # Update the registry of processed documents
        update_processed_document_ids(processed_ids)
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Document retrieval completed successfully',
                'documentsProcessed': len(downloaded_documents),
                'documents': downloaded_documents,
                'processingDetails': {
                    'visa_categories': list(PETITION_TYPES.keys()),
                    'previous_month': f"{last_month_info['month_name']} {last_month_info['year']}",
                    'total_documents_found': processed_files_count
                }
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