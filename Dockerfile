FROM python:3.12

# Install FFmpeg
RUN apt-get update

# Your application setup here...
# Adjust the COPY command to copy files into /app/src
COPY src/ /app/src/

# Set the working directory to /app
WORKDIR /app

# Install required Python packages
RUN pip install -r src/requirements.txt

# Expose the port your app runs on
EXPOSE 8080

# Adjust the CMD to run your application
CMD ["python", "src/app.py"]
