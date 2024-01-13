# Sparkscope testing

### Spark 3.2.0 JDK 8 test
update service:
```bash
aws ecs update-service \
--cluster sparkscope \
--task-definition spark-cluster \
--service sparkscope-service \
--enable-execute-command
```
*doesn't throw error, but doesnt enable execute command

run new service:
```bash
aws ecs create-service \
--cluster sparkscope \
--task-definition spark-cluster \
--service-name sparkscope-service-new \
--desired-count 1 \
--network-configuration '{ "awsvpcConfiguration": { "assignPublicIp":"ENABLED", "securityGroups": ["sg-0fba9e8f471c5763d"], "subnets": ["subnet-078fc5f880df6b37e"]}}' \
--launch-type FARGATE \
--enable-execute-command
```

describe task:
```agsl
aws ecs describe-tasks \
--cluster sparkscope \
--tasks arn:aws:ecs:us-east-1:167794223531:task/sparkscope/1342f2faf0624a16ab406656e04b8bfb
```

exec into container:
```agsl
aws ecs execute-command \
--cluster sparkscope \
--command bash \
--task arn:aws:ecs:us-east-1:167794223531:task/sparkscope/f582263f6dba422fa05bed0d2d80ac9e \
--interactive \
--container spark-worker
```
An error occurred (InvalidParameterException) when calling the ExecuteCommand operation: The execute command failed because execute command was not enabled when the task was run or the execute command agent isn’t running. Wait and try again or run a new task with execute command enabled and try again.