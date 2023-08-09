package com.harri.task2.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "countries")
public class Country {

    @Id
    @Column(length = 2, nullable = false)
    private String code;
    private String name;
    private String fullName;
    @Column(length = 3, nullable = false)
    private String iso3;
    private Short number;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "continent_code", referencedColumnName = "code")
    private Continent continent;
}
