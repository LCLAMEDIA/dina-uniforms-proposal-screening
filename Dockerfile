FROM python:3.12

# Install FFmpeg
RUN apt-get update && apt-get install -y ffmpeg && apt-get clean

# Set the working directory to /app
WORKDIR /app

# Copy the entire src directory
COPY src/ /app/src/

# Install required Python packages
RUN pip install --no-cache-dir -r src/requirements.txt

# Expose the port your app runs on
EXPOSE 8080

# Run the application
CMD ["python", "src/app.py"]