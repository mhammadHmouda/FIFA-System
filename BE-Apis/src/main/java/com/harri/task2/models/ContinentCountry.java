package com.harri.task2.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "continent-countries")
public class ContinentCountry {

    @Id
    private String country;
    private String continent;

}
