import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as path from 'path';
import { spawnSync } from 'child_process';

export class PLegalAssistStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create IAM role for API Gateway CloudWatch logging
    const apiGatewayLoggingRole = new iam.Role(this, 'ApiGatewayLoggingRole', {
      assumedBy: new iam.ServicePrincipal('apigateway.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonAPIGatewayPushToCloudWatchLogs')
      ]
    });

    // Create account-level settings for API Gateway
    const apiGatewayAccount = new apigateway.CfnAccount(this, 'ApiGatewayAccount', {
      cloudWatchRoleArn: apiGatewayLoggingRole.roleArn
    });

    // Create CloudWatch Log Group
    const logGroup = new logs.LogGroup(this, 'PLegalAssistLogs', {
      logGroupName: '/aws/lambda/plegal-assist',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // S3 bucket for raw documents - moved up to make it available for both Lambdas
    const rawDocumentsBucket = new s3.Bucket(this, 'RawDocumentsBucket', {
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: 'ArchiveAfter90Days',
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(90)
            }
          ]
        }
      ]
    });

    // Create Lambda IAM role
    const lambdaRole = new iam.Role(this, 'PLegalAssistLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    // Add required permissions
    lambdaRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );

    // Add S3 access to the main Lambda role
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:GetObject',
        's3:ListBucket'
      ],
      resources: [
        rawDocumentsBucket.bucketArn,
        `${rawDocumentsBucket.bucketArn}/*`
      ]
    }));

    // Add Bedrock model invocation permissions
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'bedrock:InvokeModel',
        'bedrock:InvokeModelWithResponseStream',
        'bedrock:CreateInferenceProfile'
      ],
      resources: [
        'arn:aws:bedrock:*::foundation-model/*',
        'arn:aws:bedrock:*:*:inference-profile/*',
        'arn:aws:bedrock:*:*:application-inference-profile/*'
      ]
    }));

    // Additional permissions for managing inference profiles
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'bedrock:GetInferenceProfile',
        'bedrock:ListInferenceProfiles',
        'bedrock:DeleteInferenceProfile',
        'bedrock:TagResource',
        'bedrock:UntagResource',
        'bedrock:ListTagsForResource'
      ],
      resources: [
        'arn:aws:bedrock:*:*:inference-profile/*',
        'arn:aws:bedrock:*:*:application-inference-profile/*'
      ]
    }));

    // Add Bedrock Knowledge Base permissions
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'bedrock:Retrieve',
        'bedrock:RetrieveAndGenerate',
      ],
      resources: [
        `arn:aws:bedrock:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:knowledge-base/BYASZZZFRM`
      ]
    }));

    // Create Lambda function with local bundling - including retrieval_function.py
    const lambdaFn = new lambda.Function(this, 'PLegalAssistFunction', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_handlers.lambda_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          local: {
            tryBundle(outputDir: string) {
              const pip = spawnSync('pip3', [
                'install',
                '--target', outputDir,
                '-r', path.join(__dirname, '../lambda/requirements.txt')
              ]);

              if (pip.error || pip.status !== 0) {
                console.error('Failed to install dependencies:', pip.error || pip.stderr.toString());
                return false;
              }

              // Copy all Python files - add retrieval_function.py to the list
              const pythonFiles = [
                'lambda_handlers.py',
                'eb1a_processor.py',
                'resume_analyzer.py',
                'kb_retriever.py',
                'retrieval_function.py'  // Added the new file
              ];
              
              for (const file of pythonFiles) {
                const copy = spawnSync('cp', [
                  path.join(__dirname, '../lambda', file),
                  outputDir
                ]);
                
                if (copy.error || copy.status !== 0) {
                  console.error(`Failed to copy ${file}:`, copy.error || copy.stderr.toString());
                  return false;
                }
              }

              return true;
            }
          },
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp lambda_handlers.py eb1a_processor.py resume_analyzer.py kb_retriever.py retrieval_function.py /asset-output/'
          ]
        }
      }),
      role: lambdaRole,
      timeout: cdk.Duration.seconds(120),
      memorySize: 1024,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        LOG_LEVEL: 'INFO',
        KNOWLEDGE_BASE_ID: 'BYASZZZFRM'
      },
      logGroup: logGroup
    });

    const api = new apigateway.RestApi(this, 'PLegalAssistApi', {
      restApiName: 'PLegal Assist API',
      description: 'API for Legal Document Analysis',
      binaryMediaTypes: ['multipart/form-data','application/pdf'],
      endpointTypes: [apigateway.EndpointType.REGIONAL],
      defaultMethodOptions: {
        requestParameters: {
          'method.request.header.Content-Type': true
        }
      },
      deployOptions: {
        stageName: 'prod',
        loggingLevel: apigateway.MethodLoggingLevel.INFO,
        dataTraceEnabled: true,
        metricsEnabled: true,
        tracingEnabled: true
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS
      }
    });

    // Ensure API Gateway account settings are configured before the API
    api.node.addDependency(apiGatewayAccount);

    // Add API Gateway resource and method
    const evaluate = api.root.addResource('evaluate');
    evaluate.addMethod('POST', new apigateway.LambdaIntegration(lambdaFn, {
      proxy: true,
      contentHandling: apigateway.ContentHandling.CONVERT_TO_BINARY,
      timeout: cdk.Duration.seconds(60),
      integrationResponses: [
        {
          statusCode: '200',
          responseParameters: {
            'method.response.header.Access-Control-Allow-Origin': "'*'"
          }
        },
        {
          statusCode: '400',
          selectionPattern: '400',
          responseParameters: {
            'method.response.header.Access-Control-Allow-Origin': "'*'"
          }
        },
        {
          statusCode: '500',
          selectionPattern: '500',
          responseParameters: {
            'method.response.header.Access-Control-Allow-Origin': "'*'"
          }
        },
        {
          statusCode: '504',
          selectionPattern: '.*TimeoutException.*',
          responseParameters: {
            'method.response.header.Access-Control-Allow-Origin': "'*'"
          }
        }
      ]
    }),
    {
        methodResponses: [
          {
            statusCode: '200',
            responseParameters: {
              'method.response.header.Access-Control-Allow-Origin': true
            }
          },
          {
            statusCode: '400',
            responseParameters: {
              'method.response.header.Access-Control-Allow-Origin': true
            }
          },
          {
            statusCode: '500',
            responseParameters: {
              'method.response.header.Access-Control-Allow-Origin': true
            }
          },
          {
            statusCode: '504',
            responseParameters: {
              'method.response.header.Access-Control-Allow-Origin': true
            }
          }
        ]
    });

    // Output the API URL
    new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
      description: 'API Gateway endpoint URL'
    });

    // Create IAM role for Document Retrieval Lambda
    const documentRetrievalRole = new iam.Role(this, 'DocumentRetrievalRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });

    // Add S3 write permissions
    documentRetrievalRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:PutObject',
        's3:GetObject',
        's3:PutObjectTagging',
        's3:HeadObject'
      ],
      resources: [
        `${rawDocumentsBucket.bucketArn}/*`,
        rawDocumentsBucket.bucketArn
      ]
    }));

    // Create the Document Retrieval Lambda with proper bundling
    const documentRetrievalLambda = new lambda.Function(this, 'DocumentRetrievalLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'retrieval_function.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          local: {
            tryBundle(outputDir: string) {
              const pip = spawnSync('pip3', [
                'install',
                '--target', outputDir,
                '-r', path.join(__dirname, '../lambda/requirements.txt')
              ]);

              if (pip.error || pip.status !== 0) {
                console.error('Failed to install dependencies:', pip.error || pip.stderr.toString());
                return false;
              }

              // Copy just the retrieval_function.py
              const copy = spawnSync('cp', [
                path.join(__dirname, '../lambda/retrieval_function.py'),
                outputDir
              ]);
              
              if (copy.error || copy.status !== 0) {
                console.error('Failed to copy retrieval_function.py:', copy.error || copy.stderr.toString());
                return false;
              }

              return true;
            }
          },
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp retrieval_function.py /asset-output/'
          ]
        }
      }),
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      role: documentRetrievalRole,
      environment: {
        RAW_BUCKET_NAME: rawDocumentsBucket.bucketName
      }
    });

    // Create Step Functions tasks
    const retrieveDocumentsTask = new tasks.LambdaInvoke(this, 'RetrieveDocuments', {
      lambdaFunction: documentRetrievalLambda,
      outputPath: '$.Payload'
    });

    // Create success and failure states
    const successState = new sfn.Succeed(this, 'DocumentRetrievalSuccess');
    const failureState = new sfn.Fail(this, 'DocumentRetrievalFailure', {
      cause: 'Document retrieval task failed',
      error: 'RetrievalError'
    });

    // Create the state machine definition using proper catch syntax
    // This properly connects the addCatch and ensures compilation
    const definition = retrieveDocumentsTask
      .addCatch(failureState)  // Add the catch first
      .next(successState);     // Then chain to success state

    // Create the state machine
    const documentRetrievalStateMachine = new sfn.StateMachine(this, 'DocumentRetrievalWorkflow', {
      definition,
      timeout: cdk.Duration.minutes(15)
    });

    // Output the state machine ARN
    new cdk.CfnOutput(this, 'DocumentRetrievalStateMachineArn', {
      value: documentRetrievalStateMachine.stateMachineArn,
      description: 'Document retrieval state machine ARN'
    });

    //
    // NEW MULTI-MONTH DOCUMENT RETRIEVAL LAMBDA
    //

    // Create CloudWatch Log Group for Multi-Month Lambda
    const multiMonthLogGroup = new logs.LogGroup(this, 'MultiMonthRetrievalLogs', {
      logGroupName: '/aws/lambda/multi-month-retrieval',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create IAM role for Multi-Month Document Retrieval Lambda
    const multiMonthRetrievalRole = new iam.Role(this, 'MultiMonthRetrievalRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ]
    });

    // Add S3 permissions (same as for the regular document retrieval Lambda)
    multiMonthRetrievalRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:PutObject',
        's3:GetObject',
        's3:PutObjectTagging',
        's3:HeadObject',
        's3:ListBucket'
      ],
      resources: [
        `${rawDocumentsBucket.bucketArn}/*`,
        rawDocumentsBucket.bucketArn
      ]
    }));

    // Create the Multi-Month Document Retrieval Lambda
    const multiMonthRetrievalLambda = new lambda.Function(this, 'MultiMonthRetrievalLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'multi_month_retrieval.multi_month_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          local: {
            tryBundle(outputDir: string) {
              const pip = spawnSync('pip3', [
                'install',
                '--target', outputDir,
                '-r', path.join(__dirname, '../lambda/requirements.txt')
              ]);

              if (pip.error || pip.status !== 0) {
                console.error('Failed to install dependencies:', pip.error || pip.stderr.toString());
                return false;
              }

              // Copy the multi_month_retrieval.py file
              const copy = spawnSync('cp', [
                path.join(__dirname, '../lambda/multi_month_retrieval.py'),
                outputDir
              ]);
              
              if (copy.error || copy.status !== 0) {
                console.error('Failed to copy multi_month_retrieval.py:', copy.error || copy.stderr.toString());
                return false;
              }

              return true;
            }
          },
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp multi_month_retrieval.py /asset-output/'
          ]
        }
      }),
      timeout: cdk.Duration.minutes(15), // Longer timeout for multiple months
      memorySize: 1024,
      role: multiMonthRetrievalRole,
      environment: {
        RAW_BUCKET_NAME: rawDocumentsBucket.bucketName,
        LOG_LEVEL: 'INFO'
      },
      logGroup: multiMonthLogGroup
    });

    // Output the Multi-Month Lambda ARN
    new cdk.CfnOutput(this, 'MultiMonthRetrievalLambdaArn', {
      value: multiMonthRetrievalLambda.functionArn,
      description: 'Multi-Month Document Retrieval Lambda ARN'
    });
  }
}