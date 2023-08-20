package com.harri.task2.services;

import com.harri.task2.models.ContinentCountry;
import com.harri.task2.models.Country;
import com.harri.task2.repositories.ContinentCountryRepository;
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
    private final ContinentCountryRepository continentCountryRepository;

    public List<Country> getCountries(int page, int size){
        Pageable pageable = PageRequest.of(page, size);
        Page<Country> countries = countryRepository.findAll(pageable);

        return countries.getContent();
    }

    public String getContinentByCountry(String country) {
        return countryRepository.findByName(country).getContinent().getName();
    }

    public List<ContinentCountry> load(List<String> countries) {
//        if(continentCountryRepository.findAll().size() > 100)
//            return continentCountryRepository.findAll();

        int batchSize = 10;
        List<ContinentCountry> batch = new ArrayList<>();

        countries.forEach(country -> {
            try {
                String continent = countryRepository.findByName(country).getContinent().getName();
                batch.add(new ContinentCountry(country, continent));

//                if (batch.size() >= batchSize) {
//                    continentCountryRepository.saveAll(batch);
//                    batch.clear();
//                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });

        return batch;

//        if (!batch.isEmpty()) {
//            continentCountryRepository.saveAll(batch);
//        }
//        return continentCountryRepository.findAll();
    }

}
