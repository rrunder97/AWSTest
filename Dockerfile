# Use AWS Lambda Python Base Image
FROM public.ecr.aws/lambda/python:3.9

# Copy application code into container
COPY app/ /var/task/

# Install dependencies
RUN pip install -r /var/task/requirements.txt

# Set the Lambda handler function
CMD ["main.lambda_handler"]

