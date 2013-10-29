package test

class GeoLocation { 
    Double lat
    Double lon

    static searchable = {
        root false
    }

    static mapping = {
        version false
    }

    static constrains = {
        lat nullable:false
        lon nullable:false
    }
}
