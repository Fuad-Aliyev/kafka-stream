package com.course.kafka.broker.message;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class CustomerPreferenceAggregateMessage {
    private Map<String, String> wishListItems;
    private Map<String, String> shoppingCartItems;

    public CustomerPreferenceAggregateMessage() {
        this.wishListItems = new HashMap<>();
        this.shoppingCartItems = new HashMap<>();
    }

    public void putWishListItem(String itemName, LocalDateTime lastDateTime) {
        this.wishListItems.put(itemName, DateTimeFormatter.ISO_DATE_TIME.format(lastDateTime));
    }

    public void putShoppingCartItem(String itemName, LocalDateTime lastDateTime) {
        this.shoppingCartItems.put(itemName, DateTimeFormatter.ISO_DATE_TIME.format(lastDateTime));
    }

    public Map<String, String> getWishListItems() {
        return wishListItems;
    }

    public void setWishListItems(Map<String, String> wishListItems) {
        this.wishListItems = wishListItems;
    }

    public Map<String, String> getShoppingCartItems() {
        return shoppingCartItems;
    }

    public void setShoppingCartItems(Map<String, String> shoppingCartItems) {
        this.shoppingCartItems = shoppingCartItems;
    }

    @Override
    public String toString() {
        return "CustomerPreferenceAggregateMessage{" +
                "wishListItems=" + wishListItems +
                ", shoppingCartItems=" + shoppingCartItems +
                '}';
    }
}
