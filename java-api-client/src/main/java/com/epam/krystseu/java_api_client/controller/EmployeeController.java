package com.epam.krystseu.java_api_client.controller;

import com.epam.krystseu.java_api_client.dto.Employee;
import com.epam.krystseu.java_api_client.service.EmployeeService;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;

@Slf4j
@RestController
@RequestMapping("/api/employees")
public class EmployeeController {

    private final EmployeeService employeeService;

    @Autowired
    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    @Operation(summary = "Get all employees", description = "Retrieves a list of all employees")
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public Employee[] getAllEmployees() throws IOException {
        return employeeService.getAllEmployees();
    }

    @Operation(summary = "Get employee by ID", description = "Retrieves a specific employee by their ID")
    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<Employee> getEmployeeById(@PathVariable String id) throws IOException {
        log.info("Request to retrieve employee by ID: {}", id);
        Employee employee = employeeService.getEmployeeById(id);
        return ResponseEntity.ok(employee);
    }

    @Operation(summary = "Search employees", description = "Searches for employees based on a specific field and value")
    @GetMapping("/search")
    @ResponseStatus(HttpStatus.OK)
    public Employee[] searchEmployees(@RequestParam String fieldName, @RequestParam String fieldValue) throws IOException {
        log.info("Request to search employees by {}: {}", fieldName, fieldValue);
        Employee[] employees = employeeService.searchEmployees(fieldName, fieldValue);
        log.info("Successfully searched employees by {}: {}", fieldName, fieldValue);
        return employees;
    }

    @Operation(summary = "Create a new employee", description = "Creates a new employee record")
    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.CREATED)
    public void createEmployee(@PathVariable String id, @RequestBody Employee employee) throws IOException {
        log.info("Request to create a new employee with ID: {}", id);
        employeeService.createEmployee(id, employee);
    }

    @Operation(summary = "Delete employee by ID", description = "Deletes a specific employee by their ID")
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteEmployeeById(@PathVariable String id) throws IOException {
        log.info("Request to delete employee with ID: {}", id);
        employeeService.deleteEmployeeById(id);
    }

    @Operation(summary = "Perform filtered terms aggregation", description = "Performs a filtered terms aggregation on a specified field")
    @PostMapping("/aggregation")
    @ResponseStatus(HttpStatus.OK)
    public Object performAggregation(
            @RequestParam String field,
            @RequestParam String filterField,
            @RequestParam Object filterValue,
            @RequestParam(defaultValue = "10") int size) throws IOException {
        log.info("Request to perform filtered terms aggregation on field: {} with filter on field: {}", field, filterField);
        return employeeService.performAggregation(field, filterField, filterValue, size);
    }
}