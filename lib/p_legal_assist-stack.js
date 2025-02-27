"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PLegalAssistStack = void 0;
const cdk = require("aws-cdk-lib");
const lambda = require("aws-cdk-lib/aws-lambda");
const apigateway = require("aws-cdk-lib/aws-apigateway");
const iam = require("aws-cdk-lib/aws-iam");
const logs = require("aws-cdk-lib/aws-logs");
const path = require("path");
class PLegalAssistStack extends cdk.Stack {
    constructor(scope, id, props) {
        super(scope, id, props);
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
        lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
        // Add Bedrock permissions
        lambdaRole.addToPolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
                'bedrock:InvokeModel'
            ],
            resources: ['*']
        }));
        // Create Lambda function with simple asset packaging
        const lambdaFn = new lambda.Function(this, 'PLegalAssistFunction', {
            runtime: lambda.Runtime.PYTHON_3_9,
            handler: 'lambda_function.lambda_handler',
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
            role: lambdaRole,
            timeout: cdk.Duration.seconds(30),
            memorySize: 256,
            environment: {
                LOG_LEVEL: 'INFO'
            },
            logGroup: logGroup
        });
        // Create API Gateway
        const api = new apigateway.RestApi(this, 'PLegalAssistApi', {
            restApiName: 'PLegal Assist API',
            description: 'API for Legal Document Analysis',
            deployOptions: {
                stageName: 'prod',
                loggingLevel: apigateway.MethodLoggingLevel.INFO,
                dataTraceEnabled: true,
                metricsEnabled: true
            },
            defaultCorsPreflightOptions: {
                allowOrigins: apigateway.Cors.ALL_ORIGINS,
                allowMethods: apigateway.Cors.ALL_METHODS
            }
        });
        // Add API Gateway resource and method
        const evaluate = api.root.addResource('evaluate');
        evaluate.addMethod('POST', new apigateway.LambdaIntegration(lambdaFn, {
            proxy: true
        }));
        // Output the API URL
        new cdk.CfnOutput(this, 'ApiUrl', {
            value: api.url,
            description: 'API Gateway endpoint URL'
        });
    }
}
exports.PLegalAssistStack = PLegalAssistStack;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicF9sZWdhbF9hc3Npc3Qtc3RhY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyJwX2xlZ2FsX2Fzc2lzdC1zdGFjay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFBQSxtQ0FBbUM7QUFFbkMsaURBQWlEO0FBQ2pELHlEQUF5RDtBQUN6RCwyQ0FBMkM7QUFDM0MsNkNBQTZDO0FBQzdDLDZCQUE2QjtBQUU3QixNQUFhLGlCQUFrQixTQUFRLEdBQUcsQ0FBQyxLQUFLO0lBQzlDLFlBQVksS0FBZ0IsRUFBRSxFQUFVLEVBQUUsS0FBc0I7UUFDOUQsS0FBSyxDQUFDLEtBQUssRUFBRSxFQUFFLEVBQUUsS0FBSyxDQUFDLENBQUM7UUFFeEIsOEJBQThCO1FBQzlCLE1BQU0sUUFBUSxHQUFHLElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsa0JBQWtCLEVBQUU7WUFDM0QsWUFBWSxFQUFFLDJCQUEyQjtZQUN6QyxTQUFTLEVBQUUsSUFBSSxDQUFDLGFBQWEsQ0FBQyxTQUFTO1lBQ3ZDLGFBQWEsRUFBRSxHQUFHLENBQUMsYUFBYSxDQUFDLE9BQU87U0FDekMsQ0FBQyxDQUFDO1FBRUgseUJBQXlCO1FBQ3pCLE1BQU0sVUFBVSxHQUFHLElBQUksR0FBRyxDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsd0JBQXdCLEVBQUU7WUFDOUQsU0FBUyxFQUFFLElBQUksR0FBRyxDQUFDLGdCQUFnQixDQUFDLHNCQUFzQixDQUFDO1NBQzVELENBQUMsQ0FBQztRQUVILDJCQUEyQjtRQUMzQixVQUFVLENBQUMsZ0JBQWdCLENBQ3ZCLEdBQUcsQ0FBQyxhQUFhLENBQUMsd0JBQXdCLENBQUMsMENBQTBDLENBQUMsQ0FDekYsQ0FBQztRQUVGLDBCQUEwQjtRQUMxQixVQUFVLENBQUMsV0FBVyxDQUFDLElBQUksR0FBRyxDQUFDLGVBQWUsQ0FBQztZQUM3QyxNQUFNLEVBQUUsR0FBRyxDQUFDLE1BQU0sQ0FBQyxLQUFLO1lBQ3hCLE9BQU8sRUFBRTtnQkFDUCxxQkFBcUI7YUFDdEI7WUFDRCxTQUFTLEVBQUUsQ0FBQyxHQUFHLENBQUM7U0FDakIsQ0FBQyxDQUFDLENBQUM7UUFFSixxREFBcUQ7UUFDckQsTUFBTSxRQUFRLEdBQUcsSUFBSSxNQUFNLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxzQkFBc0IsRUFBRTtZQUNqRSxPQUFPLEVBQUUsTUFBTSxDQUFDLE9BQU8sQ0FBQyxVQUFVO1lBQ2xDLE9BQU8sRUFBRSxnQ0FBZ0M7WUFDekMsSUFBSSxFQUFFLE1BQU0sQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFLFdBQVcsQ0FBQyxDQUFDO1lBQzlELElBQUksRUFBRSxVQUFVO1lBQ2hCLE9BQU8sRUFBRSxHQUFHLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxFQUFFLENBQUM7WUFDakMsVUFBVSxFQUFFLEdBQUc7WUFDZixXQUFXLEVBQUU7Z0JBQ1gsU0FBUyxFQUFFLE1BQU07YUFDbEI7WUFDRCxRQUFRLEVBQUUsUUFBUTtTQUNuQixDQUFDLENBQUM7UUFFSCxxQkFBcUI7UUFDckIsTUFBTSxHQUFHLEdBQUcsSUFBSSxVQUFVLENBQUMsT0FBTyxDQUFDLElBQUksRUFBRSxpQkFBaUIsRUFBRTtZQUMxRCxXQUFXLEVBQUUsbUJBQW1CO1lBQ2hDLFdBQVcsRUFBRSxpQ0FBaUM7WUFDOUMsYUFBYSxFQUFFO2dCQUNiLFNBQVMsRUFBRSxNQUFNO2dCQUNqQixZQUFZLEVBQUUsVUFBVSxDQUFDLGtCQUFrQixDQUFDLElBQUk7Z0JBQ2hELGdCQUFnQixFQUFFLElBQUk7Z0JBQ3RCLGNBQWMsRUFBRSxJQUFJO2FBQ3JCO1lBQ0QsMkJBQTJCLEVBQUU7Z0JBQzNCLFlBQVksRUFBRSxVQUFVLENBQUMsSUFBSSxDQUFDLFdBQVc7Z0JBQ3pDLFlBQVksRUFBRSxVQUFVLENBQUMsSUFBSSxDQUFDLFdBQVc7YUFDMUM7U0FDRixDQUFDLENBQUM7UUFFSCxzQ0FBc0M7UUFDdEMsTUFBTSxRQUFRLEdBQUcsR0FBRyxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsVUFBVSxDQUFDLENBQUM7UUFDbEQsUUFBUSxDQUFDLFNBQVMsQ0FBQyxNQUFNLEVBQUUsSUFBSSxVQUFVLENBQUMsaUJBQWlCLENBQUMsUUFBUSxFQUFFO1lBQ3BFLEtBQUssRUFBRSxJQUFJO1NBQ1osQ0FBQyxDQUFDLENBQUM7UUFFSixxQkFBcUI7UUFDckIsSUFBSSxHQUFHLENBQUMsU0FBUyxDQUFDLElBQUksRUFBRSxRQUFRLEVBQUU7WUFDaEMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxHQUFHO1lBQ2QsV0FBVyxFQUFFLDBCQUEwQjtTQUN4QyxDQUFDLENBQUM7SUFFTCxDQUFDO0NBQ0Y7QUF6RUQsOENBeUVDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0ICogYXMgY2RrIGZyb20gJ2F3cy1jZGstbGliJztcbmltcG9ydCB7IENvbnN0cnVjdCB9IGZyb20gJ2NvbnN0cnVjdHMnO1xuaW1wb3J0ICogYXMgbGFtYmRhIGZyb20gJ2F3cy1jZGstbGliL2F3cy1sYW1iZGEnO1xuaW1wb3J0ICogYXMgYXBpZ2F0ZXdheSBmcm9tICdhd3MtY2RrLWxpYi9hd3MtYXBpZ2F0ZXdheSc7XG5pbXBvcnQgKiBhcyBpYW0gZnJvbSAnYXdzLWNkay1saWIvYXdzLWlhbSc7XG5pbXBvcnQgKiBhcyBsb2dzIGZyb20gJ2F3cy1jZGstbGliL2F3cy1sb2dzJztcbmltcG9ydCAqIGFzIHBhdGggZnJvbSAncGF0aCc7XG5cbmV4cG9ydCBjbGFzcyBQTGVnYWxBc3Npc3RTdGFjayBleHRlbmRzIGNkay5TdGFjayB7XG4gIGNvbnN0cnVjdG9yKHNjb3BlOiBDb25zdHJ1Y3QsIGlkOiBzdHJpbmcsIHByb3BzPzogY2RrLlN0YWNrUHJvcHMpIHtcbiAgICBzdXBlcihzY29wZSwgaWQsIHByb3BzKTtcblxuICAgIC8vIENyZWF0ZSBDbG91ZFdhdGNoIExvZyBHcm91cFxuICAgIGNvbnN0IGxvZ0dyb3VwID0gbmV3IGxvZ3MuTG9nR3JvdXAodGhpcywgJ1BMZWdhbEFzc2lzdExvZ3MnLCB7XG4gICAgICBsb2dHcm91cE5hbWU6ICcvYXdzL2xhbWJkYS9wbGVnYWwtYXNzaXN0JyxcbiAgICAgIHJldGVudGlvbjogbG9ncy5SZXRlbnRpb25EYXlzLlRXT19XRUVLUyxcbiAgICAgIHJlbW92YWxQb2xpY3k6IGNkay5SZW1vdmFsUG9saWN5LkRFU1RST1lcbiAgICB9KTtcblxuICAgIC8vIENyZWF0ZSBMYW1iZGEgSUFNIHJvbGVcbiAgICBjb25zdCBsYW1iZGFSb2xlID0gbmV3IGlhbS5Sb2xlKHRoaXMsICdQTGVnYWxBc3Npc3RMYW1iZGFSb2xlJywge1xuICAgICAgYXNzdW1lZEJ5OiBuZXcgaWFtLlNlcnZpY2VQcmluY2lwYWwoJ2xhbWJkYS5hbWF6b25hd3MuY29tJyksXG4gICAgfSk7XG5cbiAgICAvLyBBZGQgcmVxdWlyZWQgcGVybWlzc2lvbnNcbiAgICBsYW1iZGFSb2xlLmFkZE1hbmFnZWRQb2xpY3koXG4gICAgICAgIGlhbS5NYW5hZ2VkUG9saWN5LmZyb21Bd3NNYW5hZ2VkUG9saWN5TmFtZSgnc2VydmljZS1yb2xlL0FXU0xhbWJkYUJhc2ljRXhlY3V0aW9uUm9sZScpXG4gICAgKTtcblxuICAgIC8vIEFkZCBCZWRyb2NrIHBlcm1pc3Npb25zXG4gICAgbGFtYmRhUm9sZS5hZGRUb1BvbGljeShuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICBhY3Rpb25zOiBbXG4gICAgICAgICdiZWRyb2NrOkludm9rZU1vZGVsJ1xuICAgICAgXSxcbiAgICAgIHJlc291cmNlczogWycqJ11cbiAgICB9KSk7XG5cbiAgICAvLyBDcmVhdGUgTGFtYmRhIGZ1bmN0aW9uIHdpdGggc2ltcGxlIGFzc2V0IHBhY2thZ2luZ1xuICAgIGNvbnN0IGxhbWJkYUZuID0gbmV3IGxhbWJkYS5GdW5jdGlvbih0aGlzLCAnUExlZ2FsQXNzaXN0RnVuY3Rpb24nLCB7XG4gICAgICBydW50aW1lOiBsYW1iZGEuUnVudGltZS5QWVRIT05fM185LFxuICAgICAgaGFuZGxlcjogJ2xhbWJkYV9mdW5jdGlvbi5sYW1iZGFfaGFuZGxlcicsXG4gICAgICBjb2RlOiBsYW1iZGEuQ29kZS5mcm9tQXNzZXQocGF0aC5qb2luKF9fZGlybmFtZSwgJy4uL2xhbWJkYScpKSxcbiAgICAgIHJvbGU6IGxhbWJkYVJvbGUsXG4gICAgICB0aW1lb3V0OiBjZGsuRHVyYXRpb24uc2Vjb25kcygzMCksXG4gICAgICBtZW1vcnlTaXplOiAyNTYsXG4gICAgICBlbnZpcm9ubWVudDoge1xuICAgICAgICBMT0dfTEVWRUw6ICdJTkZPJ1xuICAgICAgfSxcbiAgICAgIGxvZ0dyb3VwOiBsb2dHcm91cFxuICAgIH0pO1xuXG4gICAgLy8gQ3JlYXRlIEFQSSBHYXRld2F5XG4gICAgY29uc3QgYXBpID0gbmV3IGFwaWdhdGV3YXkuUmVzdEFwaSh0aGlzLCAnUExlZ2FsQXNzaXN0QXBpJywge1xuICAgICAgcmVzdEFwaU5hbWU6ICdQTGVnYWwgQXNzaXN0IEFQSScsXG4gICAgICBkZXNjcmlwdGlvbjogJ0FQSSBmb3IgTGVnYWwgRG9jdW1lbnQgQW5hbHlzaXMnLFxuICAgICAgZGVwbG95T3B0aW9uczoge1xuICAgICAgICBzdGFnZU5hbWU6ICdwcm9kJyxcbiAgICAgICAgbG9nZ2luZ0xldmVsOiBhcGlnYXRld2F5Lk1ldGhvZExvZ2dpbmdMZXZlbC5JTkZPLFxuICAgICAgICBkYXRhVHJhY2VFbmFibGVkOiB0cnVlLFxuICAgICAgICBtZXRyaWNzRW5hYmxlZDogdHJ1ZVxuICAgICAgfSxcbiAgICAgIGRlZmF1bHRDb3JzUHJlZmxpZ2h0T3B0aW9uczoge1xuICAgICAgICBhbGxvd09yaWdpbnM6IGFwaWdhdGV3YXkuQ29ycy5BTExfT1JJR0lOUyxcbiAgICAgICAgYWxsb3dNZXRob2RzOiBhcGlnYXRld2F5LkNvcnMuQUxMX01FVEhPRFNcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIEFkZCBBUEkgR2F0ZXdheSByZXNvdXJjZSBhbmQgbWV0aG9kXG4gICAgY29uc3QgZXZhbHVhdGUgPSBhcGkucm9vdC5hZGRSZXNvdXJjZSgnZXZhbHVhdGUnKTtcbiAgICBldmFsdWF0ZS5hZGRNZXRob2QoJ1BPU1QnLCBuZXcgYXBpZ2F0ZXdheS5MYW1iZGFJbnRlZ3JhdGlvbihsYW1iZGFGbiwge1xuICAgICAgcHJveHk6IHRydWVcbiAgICB9KSk7XG5cbiAgICAvLyBPdXRwdXQgdGhlIEFQSSBVUkxcbiAgICBuZXcgY2RrLkNmbk91dHB1dCh0aGlzLCAnQXBpVXJsJywge1xuICAgICAgdmFsdWU6IGFwaS51cmwsXG4gICAgICBkZXNjcmlwdGlvbjogJ0FQSSBHYXRld2F5IGVuZHBvaW50IFVSTCdcbiAgICB9KTtcblxuICB9XG59XG4iXX0=