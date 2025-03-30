# Artha: AI-Powered Immigration Assessment Tool

Artha is an AI-powered assessment tool that analyzes documents for immigration petition eligibility, focusing on EB1A, EB2NIW, and O1A visa categories. The system evaluates petitions against USCIS criteria, identifies strengths and vulnerabilities, and provides specific recommendations based on previous USCIS decisions.

## Architecture Overview

Artha comprises several integrated components:

1. **Core Assessment System**
   - API Gateway for client interaction
   - Document processing Lambda for analyzing uploaded documents
   - Claude/Bedrock integration for AI-powered assessment
   - Knowledge base integration for comparative analysis

2. **Automated Document Collection System**
   - Document retrieval automation
   - Intelligent document processing
   - Metadata extraction and tagging
   - Knowledge base enrichment
   - Orchestration and monitoring

## Key Features

- **Domain-Specific Intelligence**: Contains a specialized knowledge base of actual USCIS cases
- **Criteria-Specific Analysis**: Evaluates documentation against each criterion with specific feedback
- **Continuous Learning**: Knowledge base expands with each new case
- **Customizable Knowledge**: Law firms can incorporate anonymized case history
- **Privacy-Focused**: Each firm's data is maintained in separate, secure knowledge bases

## System Components

### AWS Infrastructure

- **API Gateway**: Handles client requests, supports file uploads
- **Lambda Functions**: 
  - Document evaluation Lambda for assessing uploaded documents
  - Document retrieval Lambda for collecting USCIS decisions
  - Multi-month retrieval Lambda for backfilling historical documents
- **Step Functions**: Orchestrates document collection workflow
- **S3 Buckets**: Store raw and processed documents
  - Raw documents bucket: Stores original PDFs from USCIS
  - Processed documents bucket: Stores extracted text and metadata
- **CloudWatch**: Monitoring and logging
  - Log groups for each Lambda function
  - Metrics for API Gateway and Lambda performance
- **Bedrock**: AI model integration and knowledge base
  - Claude model for document analysis
  - Bedrock Knowledge Base for storing precedent cases

### File Structure

- **Raw Documents**: `/[VISA_CATEGORY]/[MONTH]/[DOCUMENT_ID].pdf`
- **Document Registry**: `/processed-docs-registry.json`
- **Processed Documents**: `/processed/[VISA_CATEGORY]/[MONTH]/[DOCUMENT_ID]_metadata.json`

### Processing Pipeline

1. **Document Collection**:
   - Automatically retrieves rejected petitions from USCIS website
   - Organizes documents by visa category and month
   - Maintains registry of processed documents to avoid duplication
   - Collection types:
     - Single month (latest): `retrieval_function.handler()`
     - Multiple months (backfill): `multi_month_retrieval.multi_month_handler()`
   - Document storage strategy:
     - Files stored in S3 with path structure: `{visa_category}/{month_name}/{doc_id}.pdf`
     - Registry file maintains a central record of processed document IDs

2. **Document Processing**:
   - Extracts text from PDFs using PyPDF2
   - Analyzes content using Claude API to identify relevant criteria
   - Creates structured metadata JSON for each document
   - Metadata extraction includes:
     - USCIS criteria addressed
     - Rejection reasons
     - Precedent cases cited
     - Key legal arguments
   - Process triggered by:
     - New documents added to raw bucket
     - Manual processing requests

3. **Assessment**:
   - Compares applicant evidence against USCIS criteria
   - Evaluates likelihood of approval based on knowledge base matches
   - Performs targeted knowledge base queries for each relevant criterion
   - Evaluates evidence strength for each of the 10 USCIS criteria
   - Provides specific recommendations for strengthening petitions
   - Process flow:
     1. Document uploaded through API Gateway
     2. Text extracted and analyzed
     3. Structured profile created
     4. Knowledge base queried for similar cases
     5. Assessment results returned to client
     6. Response includes success probability and improvement recommendations

## Usage

### Document Collection

The system provides two methods for document collection:

1. **Regular Monthly Collection**:
   - Collects documents from the previous month
   - Triggered via Step Function

2. **Multi-Month Collection**:
   - Retrieves documents from multiple past months
   - Useful for backfilling historical data
   - Triggered manually with configurable parameters

### Document Assessment

1. Upload documents via the API
2. Receive detailed assessment across all criteria
3. Get specific recommendations based on previous USCIS decisions

## Development Setup

### Prerequisites

- Node.js 16+
- AWS CDK v2
- AWS CLI configured
- Python 3.9+
- Required npm packages:
  - aws-cdk-lib
  - constructs
- Required Python packages (installed automatically during deployment):
  - boto3
  - PyPDF2
  - requests
  - beautifulsoup4
  - aws_lambda_powertools

### Installation

1. Clone the repository
2. Install dependencies:
   ```
   npm install
   ```

3. Configure the AWS CDK:
   ```
   cdk bootstrap aws://ACCOUNT_ID/REGION
   ```

4. Deploy:
   ```
   cdk deploy
   ```
   
### Lambda Development

For local development and testing:

1. Set up a Python virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r lambda/requirements.txt
   ```

2. Test Lambda functions locally:
   ```
   # For document retrieval
   cd lambda
   python -c "import retrieval_function; retrieval_function.handler({}, {})"
   
   # For multi-month retrieval
   cd lambda
   python -c "import multi_month_retrieval; multi_month_retrieval.multi_month_handler({'months_back': 1}, {})"
   ```

3. Deploy Lambda function changes:
   ```
   cdk deploy
   ```

### Monitoring

1. View CloudWatch Logs:
   - Regular retrieval: `/aws/lambda/PLegalAssistStack-DocumentRetrievalLambda`
   - Multi-month retrieval: `/aws/lambda/PLegalAssistStack-MultiMonthRetrievalLambda`

2. Check documents in S3:
   ```
   aws s3 ls s3://BUCKET_NAME/EB-1A/ --recursive
   ```

### Lambda Functions

- **lambda_handlers.py**: Main handler for API requests
- **eb1a_processor.py**: Processes documents for EB1A assessment
- **resume_analyzer.py**: Analyzes document content
- **kb_retriever.py**: Interacts with the knowledge base
- **retrieval_function.py**: Handles document retrieval
- **multi_month_retrieval.py**: Handles multi-month document retrieval

## Future Work

- Expansion to additional visa categories
- Enhanced metadata extraction
- Document processing pipeline improvements
- Interactive visualization of success factors
- Integration with legal case management systems

## License

Perseverance AI(https://perseveranceai.com/) - All rights reserved

## Contact

For more information, contact Rakesh: https://perseveranceai.com/#contact