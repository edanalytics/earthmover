CREATE TABLE attendance(
day DATE NOT NULL,  -- df: start=2019-08-02 end=2020-06-17
school_id INTEGER NOT NULL, -- df: size=10000
session INTEGER NOT NULL, -- df: size=20
student_id INTEGER NOT NULL, -- df: size=10000000
attended BOOLEAN, -- df: rate=0.995
duration INTEGER NOT NULL -- df: size=61 offset=30 step=30
);
