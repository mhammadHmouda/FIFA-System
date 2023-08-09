package com.harri.task2.controllers;

import com.harri.task2.services.CountryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;
import java.util.List;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final CountryService countryService;

    @GetMapping("/continents")
    public ResponseEntity<?> getContinentsByCountry(@RequestBody List<String> countries){
        List<String> results = countryService.getContinentsByCountry(countries);
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
