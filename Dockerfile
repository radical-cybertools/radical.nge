
FROM ubuntu

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get -y install gcc python python-pip

# # Use an official Python runtime as a parent image
# FROM python:2.7-slim

# Set the working directory to /app
WORKDIR /nge

# # Copy the current directory contents into the container at /app
# ADD .. /nge

# Install any needed packages specified in requirements.txt
RUN gcc -v
RUN python -V
RUN pip install --trusted-host pypi.python.org radical.nge

# Make port 80 available to the world outside this container
EXPOSE 8090

# Define environment variable
ENV RADICAL_VERBOSE DEBUG
ENV RADICAL_PROFILE TRUE

# Run app.py when the container launches
CMD ["radical-nge-service.py"]

