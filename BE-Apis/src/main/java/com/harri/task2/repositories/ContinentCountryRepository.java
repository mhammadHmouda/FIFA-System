package com.harri.task2.repositories;

import com.harri.task2.models.ContinentCountry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContinentCountryRepository extends JpaRepository<ContinentCountry, Long> {
}
