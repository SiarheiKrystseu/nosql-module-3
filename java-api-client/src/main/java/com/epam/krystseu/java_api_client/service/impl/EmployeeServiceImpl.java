package com.epam.krystseu.java_api_client.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.epam.krystseu.java_api_client.dto.Employee;
import com.epam.krystseu.java_api_client.service.EmployeeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.ResourceNotFoundException;
import org.springframework.stereotype.Service;
import co.elastic.clients.elasticsearch.core.SearchResponse;

import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

@Slf4j
@Service
public class EmployeeServiceImpl implements EmployeeService {

    private final ElasticsearchClient elasticsearchClient;

    @Autowired
    public EmployeeServiceImpl(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public Employee[] getAllEmployees() throws IOException {
        log.info("Retrieving all employees");

        // Perform a search request to get all employees
        SearchResponse<Employee> searchResponse = elasticsearchClient.search(s -> s
                        .index("employees")
                        .size(1000)
                        .query(q -> q
                                .matchAll(m -> m)
                        ),
                Employee.class
        );

        List<Employee> employeeList = new ArrayList<>();
        for (Hit<Employee> hit : searchResponse.hits().hits()) {
            Employee employee = hit.source();
            String id = hit.id();
            employee.setId(id);
            employeeList.add(employee);
        }

        log.info("Successfully retrieved {} employees", employeeList.size());

        return employeeList.toArray(new Employee[0]);
    }



    @Override
    public Employee getEmployeeById(String id) throws IOException {
        log.info("Retrieving employee with ID: {}", id);

        Employee[] foundEmployees = searchEmployees("_id", id);

        if (foundEmployees.length == 0) {
            throw new ResourceNotFoundException("Employee not found with ID: " + id);
        }

        return foundEmployees[0];
    }


    @Override
    public void createEmployee(String id, Employee employee) throws IOException {
        log.info("Creating employee with ID: {}", id);
        IndexRequest<Employee> request = IndexRequest.of(i -> i
                .index("employees")
                .id(id)
                .document(employee)
        );

        elasticsearchClient.index(request);
        log.info("Successfully created employee with ID: {}", id);
    }

    @Override
    public void deleteEmployeeById(String id) throws IOException {
        log.info("Deleting employee with ID: {}", id);
        DeleteRequest request = DeleteRequest.of(d -> d
                .index("employees")
                .id(id)
        );

        elasticsearchClient.delete(request);
        log.info("Employee with ID: {} deleted successfully", id);
    }


    @Override
    public Employee[] searchEmployees(String fieldName, String fieldValue) throws IOException {
        log.info("Searching employees by {}: {}", fieldName, fieldValue);

        SearchResponse<Employee> searchResponse = elasticsearchClient.search(s -> s
                        .index("employees")
                        .size(1000)
                        .query(q -> q
                                .match(m -> m
                                        .field(fieldName)
                                        .query(fieldValue)
                                )
                        ),
                Employee.class
        );

        List<Employee> employeeList = new ArrayList<>();
        for (Hit<Employee> hit : searchResponse.hits().hits()) {
            Employee employee = hit.source();
            String id = hit.id();
            employee.setId(id);
            employeeList.add(employee);
        }

        log.info("Successfully found {} employees matching {}: {}", employeeList.size(), fieldName, fieldValue);

        return employeeList.toArray(new Employee[0]);
    }

    @Override
    public Object performAggregation(String field, String filterField, Object filterValue, int size) throws IOException {
        log.info("Performing filtered terms aggregation on field {} with filter {}", field, filterField);

        SearchResponse<Void> aggregationResponse = elasticsearchClient.search(s -> s
                        .index("employees")
                        .size(0)  // No need to retrieve documents
                        .query(q -> q
                                .bool(b -> b
                                        .filter(f -> f
                                                .term(t -> t
                                                        .field(filterField)
                                                        .value(getTypedValue(filterValue))
                                                )
                                        )
                                )
                        )
                        .aggregations("filtered_terms", a -> a
                                .terms(t -> t
                                        .field(field)
                                        .size(size)
                                )
                        ),
                Void.class
        );

        List<StringTermsBucket> buckets = aggregationResponse.aggregations()
                .get("filtered_terms")
                .sterms()
                .buckets().array();

        List<Map<String, Object>> result = new ArrayList<>();
        for (StringTermsBucket bucket : buckets) {
            Map<String, Object> bucketResult = new HashMap<>();

            FieldValue keyFieldValue = bucket.key();
            String key = keyFieldValue.stringValue();

            bucketResult.put("key", key);
            bucketResult.put("docCount", bucket.docCount());
            result.add(bucketResult);

            log.info("Field: {}, Doc Count: {}", key, bucket.docCount());
        }

        return result;
    }


    // Helper method to determine the value type
    private FieldValue getTypedValue(Object filterValue) {
        if (filterValue instanceof String) {
            return FieldValue.of((String) filterValue);
        } else if (filterValue instanceof Boolean) {
            return FieldValue.of(filterValue);
        } else if (filterValue instanceof Integer) {
            return FieldValue.of(filterValue);
        } else if (filterValue instanceof Double) {
            return FieldValue.of(filterValue);
        }
        throw new IllegalArgumentException("Unsupported filter value type: " + filterValue.getClass());
    }
}