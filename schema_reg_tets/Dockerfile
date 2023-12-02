# Use an official Python runtime as the base image
FROM python:latest

# Set the working directory in the container
WORKDIR /app

# Copy the requeremrnts file into the contaoner app
COPY requirements.txt /app/

# Install any needed packages specified in requiremrnts.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY ./ /app/

CMD ["python", "spark_ss.py"]
#CMD ["python", "app.py"]