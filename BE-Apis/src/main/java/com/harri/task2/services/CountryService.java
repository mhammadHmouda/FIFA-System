package com.harri.task2.services;

import com.harri.task2.models.Country;
import com.harri.task2.repositories.CountryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CountryService {

    private final CountryRepository countryRepository;


    public List<Tuple2<String, String>> getContinentsByCountry(List<String> countries) {
        List<Tuple2<String, String>> results = new ArrayList<>();

        countries.forEach(countryName -> {
            Country country = countryRepository.findByName(countryName);
            results.add(new Tuple2<>(countryName, country.getContinent().getName()));
        });

        return results;
    }

    public List<Country> getCountries(int page, int size){
        Pageable pageable = PageRequest.of(page, size);
        Page<Country> countries = countryRepository.findAll(pageable);

        return countries.getContent();
    }

    public String getContinentByCountry(String country) {
        return countryRepository.findByName(country).getContinent().getName();
    }
}