FROM maven

RUN mkdir app
RUN git clone https://github.com/spring-projects/spring-petclinic.git
RUN cd spring-petclinic && mvn --batch-mode package
RUN cd spring-petclinic && cp target/*.jar /app/
RUN cd /app/ && wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.11.1/opentelemetry-javaagent.jar

CMD java -javaagent:/app/opentelemetry-javaagent.jar \
    -Dotel.service.name=petclinic \
    -Dotel.resource.attributes=application=pet-app  \
    -Dotel.exporter.otlp.endpoint=http://opentel-proxy:4317 \
    -jar app/spring-petclinic*.jar
