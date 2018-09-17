package com.example.demo.es.dao;

import com.example.demo.es.model.Company;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CompanyRepository extends ElasticsearchRepository<Company, String> {

}
