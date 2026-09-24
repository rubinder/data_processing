# Web Server AWS

Deploys the FastAPI web server (from `web_server_code`) to AWS using ECS Fargate behind an Application Load Balancer.

## Architecture

The CloudFormation template (`cloudformation/web_server.yaml`) provisions:

- **ECR Repository** - Stores Docker images with scan-on-push and lifecycle policy (keeps last 5 images)
- **ECS Cluster** - Fargate cluster running the web server
- **Task Definition** - Fargate task (256 CPU, 512 MiB memory) with CloudWatch logging
- **ECS Service** - Runs 2 task replicas behind the ALB
- **Application Load Balancer** - Internet-facing ALB on port 80
- **Security Groups** - ALB allows inbound HTTP; ECS tasks only accept traffic from the ALB
- **IAM Roles** - Task execution role and task role
- **CloudWatch Logs** - Log group with 30-day retention


How `deploy.sh deploy` builds the image, pushes it to ECR and creates or updates the stack that `cloudformation/web_server.yaml` defines.

```mermaid
flowchart LR
    subgraph src["other repo modules"]
        code["web_server_code<br/>build context"]
        dockerfile["web_server_local/Dockerfile"]
    end
    subgraph module["web_server_aws"]
        env[(".env<br/>AWS_ACCOUNT_ID, AWS_REGION,<br/>VPC_ID, SUBNET_IDS")]
        deploy["deploy.sh<br/>build, push, create-stack,<br/>update-stack, status, logs"]
        cfn["cloudformation/web_server.yaml<br/>stack data-processing-web-server"]
    end
    subgraph aws["AWS resources provisioned by the stack"]
        ecr{{"ECR data-processing-web-server<br/>scan on push, keep last 5"}}
        alb{{"ALB data-processing-web-server-alb<br/>internet-facing, listener :80"}}
        tg{{"TargetGroup data-processing-web-tg<br/>ip targets, health /docs"}}
        svc{{"ECS Service data-processing-web-server-svc<br/>Fargate, DesiredCount 2"}}
        task{{"TaskDefinition web-server<br/>256 CPU, 512 MiB, port 8000"}}
        logs{{"CloudWatch /ecs/data-processing-web-server<br/>30-day retention"}}
        sg{{"security groups<br/>ALB 80 from anywhere, ECS 8000 from ALB"}}
        iam{{"IAM exec role + task role"}}
    end
    client["HTTP client<br/>ALBEndpoint output"]
    env -.->|"sourced"| deploy
    code -->|"docker build"| deploy
    dockerfile -->|"-f"| deploy
    deploy -->|"docker push"| ecr
    deploy -->|"aws cloudformation"| cfn
    cfn -->|"ContainerImage parameter"| task
    ecr -->|"image URI"| task
    client -->|"http :80"| alb
    alb -->|"forward"| tg
    tg -->|"HTTP :8000"| svc
    svc -->|"runs 2 tasks"| task
    task -->|"awslogs"| logs
    sg -.->|"attached"| alb
    sg -.->|"attached"| svc
    iam -.->|"ExecutionRoleArn, TaskRoleArn"| task
    deploy -.->|"aws logs tail"| logs
```

- The image is built from `web_server_code` with `web_server_local/Dockerfile`, so local and AWS run the same container.
- `deploy.sh deploy` chooses `create-stack` or `update-stack` by checking whether the stack already exists; `status` prints the `ALBEndpoint` output.
## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker (for building the container image)
- A `.env` file in the project root with:
  - `AWS_ACCOUNT_ID` - AWS account ID
  - `AWS_REGION` - AWS region
  - `VPC_ID` - VPC ID for ECS and ALB
  - `SUBNET_IDS` - Comma-separated public subnet IDs

## How to Deploy

```bash
# Full deployment: build image, push to ECR, create/update CloudFormation stack
./deploy.sh deploy

# Build the Docker image locally
./deploy.sh build

# Push the image to ECR
./deploy.sh push

# Create the CloudFormation stack
./deploy.sh create-stack

# Update an existing stack
./deploy.sh update-stack

# Check stack status and service URL
./deploy.sh status

# Delete the stack
./deploy.sh delete

# Tail ECS service logs
./deploy.sh logs
```

After deployment, the ALB endpoint URL is available in the CloudFormation stack outputs.
