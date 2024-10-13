package com.epam.krystseu.java_low_level_rest_client.service.impl;

import com.epam.krystseu.java_low_level_rest_client.client.CustomElasticsearchClient;
import com.epam.krystseu.java_low_level_rest_client.dto.Employee;
import com.epam.krystseu.java_low_level_rest_client.service.EmployeeService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.ResourceNotFoundException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class EmployeeServiceImpl implements EmployeeService {

    private final CustomElasticsearchClient elasticsearchClient;
    private final ObjectMapper objectMapper;

    @Autowired
    public EmployeeServiceImpl(CustomElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Employee[] getAllEmployees() throws IOException {
        Request request = new Request("GET", "/employees/_search");
        request.addParameter("pretty", "true");

        // Perform synchronous request
        Response response = elasticsearchClient.performRequest(request);

        // Parse the response to JSON node
        JsonNode jsonNode = objectMapper.readTree(response.getEntity().getContent());
        JsonNode hits = jsonNode.path("hits").path("hits");

        Employee[] employees = new Employee[hits.size()]; // Create an array for employees
        for (int i = 0; i < hits.size(); i++) {
            JsonNode source = hits.get(i).path("_source");
            String id = hits.get(i).path("_id").asText();

            // Deserialize JSON node to Employee object
            Employee employee = objectMapper.treeToValue(source, Employee.class);
            employee.setId(id); // Set the ID
            employees[i] = employee;
        }

        log.info("Successfully retrieved {} employees", employees.length);
        return employees;
    }

    @Override
    public Employee getEmployeeById(String id) throws IOException {
        log.info("Retrieving employee with ID: {}", id);
        Request request = new Request("GET", "/employees/_doc/" + id);
        Response response = elasticsearchClient.performRequest(request);

        // Check if the response status is 404
        if (response.getStatusLine().getStatusCode() == 404) {
            throw new ResourceNotFoundException("Employee not found with ID: " + id);
        }

        // Parse the response to JSON node
        JsonNode jsonNode = objectMapper.readTree(response.getEntity().getContent());
        JsonNode source = jsonNode.path("_source");

        // Deserialize JSON node to Employee object
        Employee employee = objectMapper.treeToValue(source, Employee.class);
        employee.setId(id);
        return employee;
    }


    @Override
    public void createEmployee(String id, Employee employee) throws IOException {
        log.info("Creating employee with ID: {}", id);
        Request request = new Request("PUT", "/employees/_doc/" + id);
        String jsonEmployee = objectMapper.writeValueAsString(employee);
        request.setEntity(new StringEntity(jsonEmployee, ContentType.APPLICATION_JSON));
        elasticsearchClient.performRequest(request);
        log.info("Employee with ID: {} created successfully", id);
    }

    @Override
    public void deleteEmployeeById(String id) throws IOException {
        log.info("Deleting employee with ID: {}", id);
        Request request = new Request("DELETE", "/employees/_doc/" + id);
        elasticsearchClient.performRequest(request);
        log.info("Employee with ID: {} deleted successfully", id);
    }

    @Override
    public Employee[] searchEmployees(String fieldName, String fieldValue) throws IOException {
        log.info("Searching employees by {}: {}", fieldName, fieldValue);
        String query = String.format("{\"query\":{\"match\":{\"%s\":\"%s\"}}}", fieldName, fieldValue);
        Request request = new Request("GET", "/employees/_search");
        request.setEntity(new StringEntity(query, ContentType.APPLICATION_JSON));
        Response response = elasticsearchClient.performRequest(request);

        // Parse the response to JSON node
        JsonNode jsonNode = objectMapper.readTree(response.getEntity().getContent());
        JsonNode hits = jsonNode.path("hits").path("hits");

        Employee[] employees = new Employee[hits.size()]; // Create an array for employees
        for (int i = 0; i < hits.size(); i++) {
            JsonNode source = hits.get(i).path("_source");
            String id = hits.get(i).path("_id").asText();

            // Deserialize JSON node to Employee object
            Employee employee = objectMapper.treeToValue(source, Employee.class);
            employee.setId(id); // Set the ID
            employees[i] = employee;
        }

        log.info("Successfully found {} employees matching {}: {}", employees.length, fieldName, fieldValue);
        return employees;
    }

    @Override
    public Object performAggregation(String field, String metricType, String metricField) throws IOException {
        log.info("Performing {} aggregation on field {} with metric {}", metricType, field, metricField);
        String query = String.format(
                "{\"size\": 0, \"aggs\": {\"%s\": {\"%s\": {\"field\": \"%s\"}}}}",
                field, metricType, metricField);
        Request request = new Request("POST", "/employees/_search");
        request.setEntity(new StringEntity(query, ContentType.APPLICATION_JSON));
        Response response = elasticsearchClient.performRequest(request);

        // Return the aggregation results directly as an Object
        return objectMapper.readValue(response.getEntity().getContent(), Object.class);
    }
}
