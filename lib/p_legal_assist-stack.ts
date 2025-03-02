import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
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

    // Create Lambda IAM role
    const lambdaRole = new iam.Role(this, 'PLegalAssistLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    // Add required permissions
    lambdaRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );

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
        'bedrock:RetrieveAndGenerate',  // Add this new permission
      ],
      resources: [
        `arn:aws:bedrock:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:knowledge-base/BYASZZZFRM`
      ]
    }));

    // Create Lambda function with local bundling
    const lambdaFn = new lambda.Function(this, 'PLegalAssistFunction', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_handlers.lambda_handler',  // Updated handler
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

              // Copy all Python files
              const pythonFiles = [
                'lambda_handlers.py',
                'eb1a_processor.py',
                'resume_analyzer.py',
                'kb_retriever.py'
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
            'pip install -r requirements.txt -t /asset-output && cp lambda_handlers.py eb1a_processor.py resume_analyzer.py kb_retriever.py /asset-output/'
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
  }
}