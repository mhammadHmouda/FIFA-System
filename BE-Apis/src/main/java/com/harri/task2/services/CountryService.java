package com.harri.task2.services;

import com.harri.task2.models.Country;
import com.harri.task2.repositories.CountryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CountryService {

    private final CountryRepository countryRepository;

    // Take a list of country and return a list of country and the continents of this country
    public List<String> getContinentsByCountry(List<String> countries) {
        List<String> results = new ArrayList<>();

        countries.forEach(countryName -> {
            Country country = countryRepository.findByName(countryName);
            results.add(countryName + ": " + country.getContinent().getName());
        });

        return results;
    }

    // Take the page number and the size of this page and return a list of this countries
    public List<Country> getCountries(int page, int size){
        Pageable pageable = PageRequest.of(page, size);
        Page<Country> countries = countryRepository.findAll(pageable);

        return countries.getContent();
    }

    // Take a country name and return the continent of this country
    public String getContinentByCountry(String country) {
        return countryRepository.findByName(country).getContinent().getName();
    }
}
