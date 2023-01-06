import sys
from functools import reduce
from itertools import islice
from operator import itemgetter
import io
from pymongo import MongoClient, InsertOne
import urllib
from datetime import datetime
import datetime as t
import json
import pymssql


def start():
    content = list()
    clientes = ['COMPFINANCIAMIENTOTUYA', 'CARDIF', 'ITAU', 'Colfondos', 'SERFINANZA', 'RCI', 'Comfandi',
                'MunicipioMedellin', 'Proteccion', 'DannRegional']
    for cliente in clientes:
        date_i = datetime.now()
        log(f'Iniciando proceso para {cliente}')
        new, update = create_slmailid(cliente)
        date_f = datetime.now()
        content.append(content_log(cliente, new, update, date_i, date_f))
    create_log(content)


def create_log(newlist2):
    content = f'{newlist2}\n'
    with io.open('output/logsame.txt', mode="a") as fd:
        fd.write(content)
        fd.close()


def content_log(cliente, new, update, date_i, date_f):
    return {
        "cliente": cliente,
        "Registros Actulizados": update,
        "Registros de Nuevo ingreo": new,
        "hora ini": date_i.strftime('%Y-%m-%d %H:%M'),
        "hora fin": date_f.strftime('%Y-%m-%d %H:%M')
    }


def create_connection_visor_database():
    # Connection MongoDB
    mongo_conn_prod = {
        "conn_str": 
        "db_name": "",
    }

    mongo_conn_qa = {
        "conn_str": 
        "db_name": "",
    }

    mongodb_client = MongoClient(mongo_conn_qa
                                 ['conn_str'])
    log('Connection - MongoClient: Open')

    return mongodb_client


def create_query(db, cliente):
    """
    this method create a query that consults the op's that are going to be processed
    :return: Query
    """
    date_b = get_date()
    query_ciclos = f"""
    SELECT DISTINCT
	    e.slmailingid
FROM (
   SELECT ca.*, ca.code + '-%' AS slmailingid FROM {db}.dbo.campaign ca
   WHERE ca.visorintegration=1 
) AS c
	JOIN {db}.dbo.customer cus 
		ON cus.id = c.customerid AND cus.name = '{cliente}'
   	JOIN {db}.dbo.emailattachmentevent AS e
   		ON e.slmailingid LIKE c.slmailingid
   		AND e.created_at BETWEEN '{date_b} 00:00:00' AND '{date_b} 23:59:59';
    """
    return query_ciclos


def get_date():
    """
    This method gives the date to be used in create_query
    :return:Period of time
    """
    ahora = t.datetime.utcnow()
    ayer = ahora - t.timedelta(days=1)
    date_b = ayer.strftime('%Y-%m-%d')
    return date_b


def create_slmailid(cliente):
    db_list = ('samev2', 'samev3', 'samev4')
    for db in db_list:
        log(f'Hora inicio base de datos {db}:')
        conn = create_conncetion(db)
        connect_same = conn.cursor()
        log("consultando ciclos")
        log('Espere...')
        connect_same.execute(create_query(db, cliente))
        list_ciclo = list()
        for ciclo in connect_same.fetchall():
            list_ciclo.append(str(ciclo[0]))
        if len(list_ciclo) == 1:
            list_ciclo.append('')
            list_ciclos = tuple(list_ciclo)
            new, update = create_vadilation_event_email(connect_same, list_ciclos)
            return new, update
        elif len(list_ciclo) >= 2:
            list_ciclos = tuple(list_ciclo)
            new, update = create_vadilation_event_email(connect_same, list_ciclos)
            return new, update
        elif len(list_ciclo) == 0:
            continue


def create_slmailid_list(filename):
    db_list = ('samev2', 'samev3', 'samev4')
    with open(filename, "r") as priority:
        list_ciclos = tuple(priority.read().split('\n'))
    for db in db_list:
        log(f'Hora inicio base de datos {db}')
        conn = create_conncetion(db)
        new, update = create_vadilation_event_email(conn.cursor(), list_ciclos)
        return new, update


def create_conncetion(db):
    return pymssql.connect(
        host='',
        database=db,
        user='',
        password='')


def execute_search_email_event(list_ciclos):
    return f"""
    SELECT 
        f.id AS 'id',
        f.UCID AS 'ucid',
        f.attachments AS 'attachments',
        f.slmailingid AS 'PE_cycleCode',
        f.slmailingid AS 'mailingId',
        f.slmessageid AS 'messageId',
        f.[EMAIL] AS 'email',
        f.emailsentdate AS 'PE_processDate',
        f.status AS 'SAME_statusCode',
        CASE f.status
            WHEN '1' THEN 'Entregado'
            WHEN '0' THEN 'Enviado'
            WHEN '2' THEN 'Fallido'
            WHEN '4' THEN 'Spam'
        ELSE 'Fallido'
        END AS 'SAME_statusName',
        CASE f.failuretype
            WHEN '0' THEN 'Rechazo temporal'
            WHEN '1' THEN 'Rechazo permanente'
            WHEN '2' THEN 'Supresion'
            WHEN '4' THEN 'Spam'
        ELSE ''
        END AS 'SAME_statusNameTipo',
        f.failurecode AS 'SAME_CodigoEstado',
        f.response AS 'SAME_response',
	    f.slmailingid + '_' +f.slmessageid +'_' + f.[EMAIL] AS message_key,
	    f.source
    FROM [emailattachmentevent] AS f WITH (NOLOCK)
    WHERE  f.slmailingid in {list_ciclos}   
    ORDER BY f.slmailingid, f.slmessageid, f.[EMAIL]
"""


def search_email_open_events(list_ciclos):
    return f"""
    SELECT
        [emailattachmentloggerid] as 'id',
        [email] as 'email',
        f.eventdate AS 'fechaapertura',
        [slmailingid] as 'maillingid',
        [slmessageid] as 'messageid',
        [ip] as 'ip',
        [url] as 'url',
        f.slmailingid + '_' + f.slmessageid + '_' + f.email AS message_key
    FROM [emailattachmentopened] AS f WITH (NOLOCK)
      WHERE f.slmailingid + '_' + f.slmessageid + '_' + f.email in (
            SELECT DISTINCT
                f.slmailingid + '_' +f.slmessageid + '_'+ f.[EMAIL] AS message_key
            FROM [emailattachmentevent] AS f WITH (NOLOCK)
            WHERE f.slmailingid in {list_ciclos}
            )
"""


def create_vadilation_event_email(connect_same, list_ciclos):
    log('Inicio de proceso, Esto puede tardar unos minutos')
    # Crear el objeto que se va filtrar
    ucid_email = set()
    slmialingid = set()
    registros = 0
    registros_update = 0
    slmialingid.add('')
    same_communication_bulk = list()
    register_errors = list()
    same_communications = dict()
    with connect_same as cursor:
        mongo_client_nt = create_connection_visor_database()
        same_communication_collection = mongo_client_nt['vudc']['SameCommunication']

        items_per_batch = 2
        for index_part in range(0, int(len(list_ciclos) / items_per_batch) + 1):
            index_init = index_part * items_per_batch
            index_end = (index_part + 1) * items_per_batch

            list_op_cadena = tuple(islice(list_ciclos, index_init, index_end))
            open('SAME_batch_in_process.csv', 'w').write(f'{list_op_cadena}')
            if len(list_op_cadena) >= 2:
                cursor.execute(execute_search_email_event(list_op_cadena))
                for email_event in cursor.fetchall():
                    if email_event[1][-1] != 'P':
                        if email_event[14] == 'FORWARD_VUDC':
                            pass
                        elif email_event[14] is not None:
                            if email_event[1] != '' and email_event[1] is not None:
                                get_same_communication(same_communications, email_event)
                                ucid_email.add(email_event[1])
                                slmialingid.add(email_event[4])
                            else:
                                register_errors.append(json.dumps(register_error(email_event)))

                # Crear evetos de apertura
                if len(slmialingid) > 1:
                    cursor.execute(search_email_open_events(tuple(slmialingid)))
                    for event_email in cursor.fetchall():
                        same_communications[event_email[7]]['sameEvent'].append(create_same_open_event(event_email))

                    map_same_communications = dict()
                    items_per_batchs = 10000
                    for index_part in range(0, int(len(ucid_email) / items_per_batchs) + 1):
                        index_initi = index_part * items_per_batchs
                        index_endi = (index_part + 1) * items_per_batchs

                        list_ucids = tuple(islice(ucid_email, index_initi, index_endi))

                        same_communications_mongo = list(
                            same_communication_collection.find({'ucid': {"$in": list(list_ucids)}}))
                        for same_communication_mongo in same_communications_mongo:
                            map_same_communications[same_communication_mongo['ucid']] = same_communication_mongo

                    for same_communication in same_communications.values():
                        try:
                            same_communication_mongo = map_same_communications[same_communication['ucid']]
                            new_same_event = dict()
                            eventos_satelite = same_communication['sameEvent']
                            eventos_mongo = same_communication_mongo['sameEvent']
                            if comparate_List_Event(eventos_satelite, eventos_mongo):
                                continue
                            else:
                                for event in same_communication['sameEvent'] + same_communication_mongo['sameEvent']:
                                    fecha = event['eventDate'].strftime('%Y-%m-%d %H:%M')
                                    event_key = f"{fecha}_{event['eventStatus']}_{event['eventCode']}"
                                    try:
                                        new_same_event[event_key]
                                    except KeyError as e:
                                        new_same_event[event_key] = event
                                same_communication_collection.update_one(
                                    {'_id': same_communication_mongo['_id']},
                                    {'$set': {'sameEvent': list(new_same_event.values())}},
                                )
                                registros_update = registros_update + 1
                        except KeyError as e:
                            del same_communication['id']
                            del same_communication['message_key']
                            same_communication_bulk.append(InsertOne(same_communication))
                            size_list = sys.getsizeof(same_communication_bulk)
                            registros = registros + 1
                            if 14000000 < size_list <= 16777216:
                                same_communication_collection.bulk_write(same_communication_bulk, ordered=False)
                                same_communication_bulk.clear()
                    if same_communication_bulk:
                        same_communication_collection.bulk_write(same_communication_bulk, ordered=False)
                        same_communication_bulk.clear()

                log('Hora fin proceso')
                if len(register_errors) >= 1:
                    crear_archivo_error(register_errors)
    return registros, registros_update


def get_same_communication(same_communication_docs: dict, email_event):
    if email_event[13] in same_communication_docs:
        if email_event[1] == same_communication_docs.get('ucid'):
            if email_event[0] >= same_communication_docs['id']:
                return same_communication_docs[email_event[13]]
    else:
        same_communication_docs[email_event[13]] = create_same_communication(email_event)
        return same_communication_docs[email_event[13]]


def comparate_List_Event(list_same: list, list_mongo: list):
    newlist = sorted(list_same, key=itemgetter('eventDate'))
    newlist2 = sorted(list_mongo, key=itemgetter('eventDate'))
    if newlist == newlist2:
        return True
    else:
        return False


def create_same_communication(email_event: list):
    hora = email_event[7]
    new_Hour = hora + t.timedelta(minutes=300)
    return dict(
        id=email_event[0],
        ucid=email_event[1],
        campaignCode=email_event[4],
        campaignName="",
        campaignStartDate=new_Hour,
        campaignEndDate=new_Hour,
        campaignStatus="Activa",
        productType="Activa",
        productStatus="Activo",
        mailingId=email_event[4],
        messageId=email_event[5],
        updated_at=new_Hour,
        loggerEndDate=new_Hour,
        email=email_event[6],
        isForwaded=False,
        attachments=json.dumps(email_event[2]),
        extraData=[],
        sameEvent=[dict(
            eventDate=new_Hour,
            eventStatus=email_event[9],
            eventCode=str(email_event[8]),
            failureType=email_event[10],
            failureCode=email_event[11],
            response=email_event[12],
            responseDes="Carga Masiva")],
        createdAt=new_Hour,
        updatedAt=new_Hour,
        message_key=email_event[13]
    )


def create_same_open_event(event_email: list):
    new_hour = event_email[2] + t.timedelta(hours=5)
    return dict(
        eventDate=new_hour,
        eventStatus="Abierto",
        eventCode="5",
        openDate=new_hour,
        openIP=event_email[5],
        openURL=event_email[6]
    )


def register_error(email_event: list):
    return dict(
        id=email_event[0],
        ucid=email_event[1],
        mailingId=email_event[4],
        messageId=email_event[5],
        email=email_event[6],
    )


def crear_archivo_error(register_errors):
    date_b = get_date()
    with open(f'erores_SAME_{date_b}.cvs'.replace(':', '-'), 'a') as archivo:
        archivo.write('\n'.join(register_errors))


def log(msg: str):
    if type(msg) == str:
        print(f'[{datetime.now()}]', msg)
    elif type(msg) == list:
        print(f'[{datetime.now()}]', reduce(lambda s1, s2: f"{s1} {s2}", f'{msg}'))
    else:
        print(f'[{datetime.now()}]', f'{msg}')


# create_slmailid_list('validar.txt')
# create_slmailid()
start()
