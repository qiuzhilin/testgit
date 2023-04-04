package com.qzl.vo;

public class Discount {
    private String customerType;
    private double discount;

    public Discount(String customerType, double discount) {
        this.customerType = customerType;
        this.discount = discount;
    }

    public String getCustomerType() {
        return customerType;
    }

    public void setCustomerType(String customerType) {
        this.customerType = customerType;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }
}
