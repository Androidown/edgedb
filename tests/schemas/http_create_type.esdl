type Person {
    property mid -> std::int64 {
        constraint std::exclusive;
    };
    property surname -> std::str ;
    property firstname -> std::str ;
    property address -> std::str ;
    property zipcode -> std::int64 ;
    property telephone -> std::str ;
    property recommendedby -> std::int64 ;
    property joindate -> cal::local_datetime ;
};
