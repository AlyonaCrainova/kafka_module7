package org.example;


import com.fasterxml.jackson.annotation.JsonProperty;


public class User {
    @JsonProperty
    private String name;
    @JsonProperty
    private int favoriteNumber;
    @JsonProperty
    private String favoriteColor;


    public User() {}


    public User(String name, int favoriteNumber, String favoriteColor) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
        this.favoriteColor = favoriteColor;
    }


    public String getName() { return name; }
    public int getFavoriteNumber() { return favoriteNumber; }
    public String getFavoriteColor() { return favoriteColor; }
}