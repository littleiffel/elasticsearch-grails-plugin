package test

class Product {
    String name
    String description = "A description of a product"
    Float price = 1.00
    GeoLocation geo

    static searchable = {
        geo geoPoint:true
    }

    static constraints = {
        name blank:false
        description nullable: true
        price nullable:true
    }
}
