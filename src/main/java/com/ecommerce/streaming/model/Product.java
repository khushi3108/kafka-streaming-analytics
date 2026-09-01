package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a product in the catalog, stored as a compacted KTable in the "products" topic.
 * Key = productId (enables KStream-KTable join for order enrichment).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Product {

    private String productId;
    private String name;
    private String category;
    private double price;
    private String brand;
    private String description;
}

