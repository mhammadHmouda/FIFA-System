package com.harri.task2.controllers;

import com.harri.task2.models.ContinentCountry;
import com.harri.task2.services.CountryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final CountryService countryService;
    @GetMapping
    public ResponseEntity<?> welcomePage(){
        return ResponseEntity.ok("Welcome to Fifa api's!");
    }
    @PostMapping("/continents")
    public ResponseEntity<?> loadContinent(@RequestBody List<String> countries){
        List<ContinentCountry> results = countryService.load(countries);
        return ResponseEntity.ok(results);
    }
    @GetMapping("/continent/{country}")
    public ResponseEntity<?> getContinentByCountry(@PathVariable String country){
        String continent = countryService.getContinentByCountry(country);
        return ResponseEntity.ok(continent);
    }
    @GetMapping("/countries")
    public ResponseEntity<?> getCountries(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "10") int size) {

        return ResponseEntity.ok(countryService.getCountries(page, size));
    }
}
