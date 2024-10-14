package com.epam.krystseu.java_api_client.controller;

import com.epam.krystseu.java_api_client.dto.Employee;
import com.epam.krystseu.java_api_client.service.EmployeeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;

@Slf4j
@Tag(name = "Employees", description = "Operations related to employees")
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
        // First, try to search for the employee by ID
        Employee[] foundEmployees = employeeService.searchEmployees("_id", id);

        // Check if any employees were found
        if (foundEmployees.length == 0) {
            return ResponseEntity.notFound().build();
        }

        // If found, retrieve the employee's full details using the ID
        Employee employee = employeeService.getEmployeeById(id);
        return ResponseEntity.ok(employee);
    }


    @Operation(summary = "Create a new employee", description = "Creates a new employee record")
    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.CREATED)
    public void createEmployee(@PathVariable String id, @RequestBody Employee employee) throws IOException {
        employeeService.createEmployee(id, employee);
    }

    @Operation(summary = "Delete employee by ID", description = "Deletes a specific employee by their ID")
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteEmployeeById(@PathVariable String id) throws IOException {
        employeeService.deleteEmployeeById(id);
    }

    @Operation(summary = "Search employees", description = "Searches for employees based on a specific field and value")
    @GetMapping("/search")
    @ResponseStatus(HttpStatus.OK)
    public Employee[] searchEmployees(@RequestParam String fieldName, @RequestParam String fieldValue) throws IOException {
        return employeeService.searchEmployees(fieldName, fieldValue);
    }

    @Operation(summary = "Perform aggregation", description = "Performs aggregation on a specified field with a given metric type")
    @PostMapping("/aggregation")
    @ResponseStatus(HttpStatus.OK)
    public Object performAggregation(@RequestParam String field, @RequestParam String metricType, @RequestParam String metricField) throws IOException {
        return employeeService.performAggregation(field, metricType, metricField);
    }
}


