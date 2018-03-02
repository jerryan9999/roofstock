CREATE TABLE property (
    id integer NOT NULL,
    source character varying,
    source_id integer,
    latitude double precision,
    longitude double precision,
    squarefeet double precision,
    bathrooms double precision,
    bedrooms double precision,
    yearbuilt integer,
    propertytype character varying,
    lotsize integer,
    ispool boolean,
    
    address1 character varying,
    zip character varying,
    city character varying,
    county character varying,
    cbsacode integer,
    state character varying(10),

    listprice double precision,
    monthlyrent double precision,
    yearlyinsurancecost double precision,
    yearlypropertytaxes double precision,
    appreciation double precision,

    neighborscore double precision,

    status character varying,
    created_at timestamp,
    updated_at timestamp,

    imgurl character varying,
    neighbor_regionid character varying,
    score_v1_appreciation character varying,
    score_v2_balance character varying,
    score_v3_return character varying,
    score_version integer
);


CREATE UNIQUE INDEX unique_property_idx ON property (source,source_id);
ALTER TABLE property ADD CONSTRAINT unique_property_constraint UNIQUE USING INDEX unique_property_idx;

