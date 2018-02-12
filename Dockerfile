
FROM ubuntu

# Define environment variables
ENV DEBIAN_FRONTEND  noninteractive
ENV RADICAL_VERBOSE  DEBUG
ENV RADICAL_PROFILE  TRUE
ENV RADICAL_NGE_HOST 0.0.0.0
ENV RADICAL_NGE_PORT 8080


# Make the port available to the world outside this container
# FIXME: how to reference $RADICAL_NGE_PORT?
EXPOSE 8080

# Set the working directory
WORKDIR /nge

# Install base system and any needed packages
RUN apt-get update && \
    apt-get -y install gcc python python-pip git

RUN gcc -v
RUN python -V
# RUN pip install --trusted-host pypi.python.org radical.nge
RUN pip install "git+https://github.com/radical-cybertools/radical.nge.git@devel"

# Run app.py when the container launches
CMD ["radical-nge-service.py"]

