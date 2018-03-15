
# ------------------------------------------------------------------------------
FROM ubuntu

# Define environment variables
ENV DEBIAN_FRONTEND  noninteractive
ENV RADICAL_VERBOSE  DEBUG
ENV RADICAL_PROFILE  TRUE
ENV RADICAL_NGE_HOST 0.0.0.0
ENV RADICAL_NGE_PORT 8080
ENV HOME             /home/radical/nge


# Make the port available to the world outside this container
# FIXME: how to reference $RADICAL_NGE_PORT?
EXPOSE 8080


# Install base system and any needed packages
RUN apt-get update && \
    apt-get -y install gcc python python-virtualenv python-pip git


# USER    radical  # this is ignored by openshift
RUN     useradd -m radical
WORKDIR /home/radical/nge

# openshift wants us to bend over backwards to actually use the image
RUN     mkdir -p         /home/radical/nge
# RUN     chgrp -R 0       /home/radical/nge
# RUN     chmod -R g+rwX   /home/radical/nge
# RUN     chgrp -R 0       /var/log
# RUN     chmod -R g+rwX   /var/log
# RUN     chgrp -R 0       /etc
# RUN     chmod -R g+rwX   /etc 

RUN chmod -R a+rwX     /[^sp]*
RUN git clone "https://github.com/radical-cybertools/radical.nge.git"; \
    ls -la             ; \
    cd radical.nge     ; \
    ls -la             ; \
    git checkout devel ; \
    pip install .

# Run app.py when the container launches
CMD ["radical-nge-service.sh"]
CMD ["/bin/bash"]

# ------------------------------------------------------------------------------

