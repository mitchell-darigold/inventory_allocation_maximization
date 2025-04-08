drop table if exists trimble_location_matrix_distances;

create table trimble_location_matrix_distances(
    customer_name TEXT,
    dest_location_gid TEXT,
    corp_grp TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    latitude TEXT,
    longitude TEXT,
    pattern1 TEXT,
    delivery_lat_lon TEXT,
    domicile_lat_lon TEXT,
    total_weight_per_week TEXT,
    open TEXT,
    close TEXT,
    frequency TEXT,
    dist_delivery_domicile TEXT,
    joiner TEXT,
    customer_name_2 TEXT,
    delivery_lat_lon_2 TEXT,
    pattern1_2 TEXt,
    open_2 TEXT,
    close_2 TEXT,
    distance_del_del REAL
    );

.mode csv
.headers off

.import 'S:\\Supply_Chain\\Analytics\\Trimble Model\\Data Comparisons\\SPO\\location dimensions distances.csv' trimble_location_matrix_distances