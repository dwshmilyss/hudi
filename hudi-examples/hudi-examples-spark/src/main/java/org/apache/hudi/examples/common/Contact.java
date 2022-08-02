package org.apache.hudi.examples.common;

import lombok.Data;

import java.util.Date;
import java.sql.Timestamp;

/**
 * @Description: TODO
 * @Author: David.duan
 * @Date: 2022/8/2
 **/
@Data
public class Contact {
    private long id;
    private long ts;
    private String date;
    private long version;
    private String anonymous_id;
    private String avatar;
    private String city;
    private String comments;
    private String company;
    private String country;
    private Timestamp date_created;
    private Timestamp date_of_birthday;
    private String email;
    private String gender;
    private String home_phone;
    private String industry;
    private Timestamp last_updated;
    private String mobile_phone;
    private String name;
    private String postal_code;
    private String state;
    private String street;
    private long tenant_id;
    private String title;
    private String user_name;
    private String website;
    private String nickname;
    private String department;
    private long merged_id;
    private byte is_anonymous;
    private String id_card;
    private String company_short_name;
    private int age;
    private long group1;
    private long group2;
    private long group3;
    private long group4;
    private long group5;
    private long group6;
    private long group7;
    private long group8;
    private long group9;
    private long group10;
    private long group11;
    private long group12;
}
