FROM python:3.9-slim

# Install FFmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Install requests
RUN pip install -r requirements.txt

# Copy the main script
COPY main.py .

# Run the script
CMD ["python", "-u", "main.py"]
