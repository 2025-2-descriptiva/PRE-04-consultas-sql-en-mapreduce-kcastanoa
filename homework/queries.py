"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import hadoop


#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# CONSULTA 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append( 
                (
                    index, 
                    row.strip() + ',tip_rate'
                )  
            )
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])            
            tip_rate = tip / total_bill
            result.append(
                (index, row.strip() + "," + str(tip_rate))
            )

    return result

def reducer_query_1(sequence):
    return sequence



#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence



#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence


#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#

def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())



#
# ORQUESTADOR:
#
def run():
    """Orquestador"""


    hadoop(
        mapper_fn=mapper_query_1,
        reducer_fn=reducer_query_1,
        input_folder="files/input/",
        output_folder="files/query_1",
    )

    hadoop(
        mapper_fn=mapper_query_2,
        reducer_fn=reducer_query_2,
        input_folder="files/input/",
        output_folder="files/query_2",
    )    

    hadoop(
        mapper_fn=mapper_query_3,
        reducer_fn=reducer_query_3,
        input_folder="files/input/",
        output_folder="files/query_3",
    )        

    hadoop(
        mapper_fn=mapper_query_4,
        reducer_fn=reducer_query_4,
        input_folder="files/input/",
        output_folder="files/query_4",
    )        

    hadoop(
        mapper_fn=mapper_query_5,
        reducer_fn=reducer_query_5,
        input_folder="files/input/",
        output_folder="files/query_5",
    )        

def run():
    """Orquestador"""
    import os, shutil

    # 0) Estructura base y limpieza mínima
    os.makedirs("files", exist_ok=True)
    os.makedirs("files/output", exist_ok=True)  # <- evita el fallo al primer hadoop

    # Si ya corriste antes, borra las carpetas de queries para evitar FileExistsError
    for d in [f"files/query_{i}" for i in range(1, 6)]:
        if os.path.exists(d):
            shutil.rmtree(d)

    # Aux: ejecutar una query y copiar su part-00000 al directorio de la query
    def run_query(mapper, reducer, out_dir):
        hadoop(
            mapper_fn=mapper,
            reducer_fn=reducer,
            input_folder="files/input/",
            output_folder=out_dir,      # crea out_dir y _SUCCESS ahí
        )
        # copiar el resultado generado (siempre cae en files/output/part-00000)
        src = "files/output/part-00000"
        dst = os.path.join(out_dir, "part-00000")
        if os.path.exists(src):
            shutil.copy(src, dst)
        else:
            raise Exception("No se generó files/output/part-00000; revisa mapper/reducer.")

    # 1) Ejecutar consultas
    run_query(mapper_query_1, reducer_query_1, "files/query_1")
    run_query(mapper_query_2, reducer_query_2, "files/query_2")
    run_query(mapper_query_3, reducer_query_3, "files/query_3")
    run_query(mapper_query_4, reducer_query_4, "files/query_4")
    run_query(mapper_query_5, reducer_query_5, "files/query_5")


if __name__ == "__main__":

    run()
