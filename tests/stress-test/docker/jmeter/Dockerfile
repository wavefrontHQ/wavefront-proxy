FROM eclipse-temurin:11

RUN apt-get update
RUN apt-get install wget
RUN wget https://dlcdn.apache.org//jmeter/binaries/apache-jmeter-5.4.3.tgz
RUN tar -xvzf apache-jmeter-5.4.3.tgz
RUN rm apache-jmeter-5.4.3.tgz

RUN mv apache-jmeter-5.4.3 /jmeter

ENV JMETER_HOME /jmeter

# Add Jmeter to the Path
ENV PATH $JMETER_HOME/bin:$PATH