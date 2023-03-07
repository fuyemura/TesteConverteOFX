import itertools as it
import camelot
import pandas as pd

from ofxtools.models import *
from ofxtools.utils import UTC
from decimal import Decimal
from datetime import datetime

# from csv2ofx import utils
# from csv2ofx.mappings.default import mapping
# from csv2ofx.ofx import OFX
# from meza.io import read_csv, IterStringIO


# def convert_to_ofx(records):
#     ofx = OFX(mapping, def_type="CHECKING")
#     groups = ofx.gen_groups(records)
#     trxns = ofx.gen_trxns(groups)
#     cleaned_trxns = ofx.clean_trxns(trxns)
#     data = utils.gen_data(cleaned_trxns)
#     content = it.chain(
#        [
#            "OFXHEADER:100\n",
#            ofx.header(),
#            ofx.gen_body(data),
#            ofx.footer(),
#        ]
#    )
#
#    return b"\n".join(IterStringIO(content))

def convert_to_ofx_teste():
    ledgerbal = LEDGERBAL(balamt=Decimal('150.65'), dtasof = datetime(2015, 1, 1, tzinfo=UTC))
    acctfrom = BANKACCTFROM(bankid='123456789',  acctid = '23456', accttype = 'CHECKING')  # OFX Section 11.3.1
    stmtrs = STMTRS(curdef='BRL', bankacctfrom=acctfrom,  ledgerbal = ledgerbal)

    status = STATUS(code=0, severity='INFO')

    stmttrnrs = STMTTRNRS(trnuid='5678', status=status, stmtrs=stmtrs)
    bankmsgsrs = BANKMSGSRSV1(stmttrnrs)
    fi = FI(org='Illuminati', fid='666')  # Required for Quicken compatibility
    sonrs = SONRS(status=status, dtserver = datetime(2015, 1, 2, 17, tzinfo=UTC), language = 'POR', fi = fi)
    signonmsgs = SIGNONMSGSRSV1(sonrs=sonrs)
    ofx = OFX(signonmsgsrsv1=signonmsgs, bankmsgsrsv1=bankmsgsrs)

    import xml.etree.ElementTree as ET
    root = ofx.to_etree()
    message = ET.tostring(root).decode()

    from ofxtools.header import make_header
    header = str(make_header(version=102))

    return (header + message)


def tratamento_dfxp(dfxp, year):
    # Elimina colunas
    dfxp[0].drop(3, axis=1, inplace=True)

    # Elimina colunas
    dfxp[1].drop({3, 4}, axis=1, inplace=True)

    dfxpfinal = pd.concat([dfxp[0], dfxp[1]])

    # Rename de colunas
    dfxpfinal.rename(columns={0: 'Data'}, inplace=True)
    dfxpfinal.rename(columns={1: 'Descricao'}, inplace=True)
    dfxpfinal.rename(columns={2: 'Valor'}, inplace=True)

    dfxpfinal['Data'] = dfxpfinal['Data'] + '/' + year

    # Definir tipo de dados
    dfxpfinal['Data'] = pd.to_datetime(dfxpfinal['Data'], format='%d/%m/%Y', errors='coerce')
    dfxpfinal['Descricao'] = dfxpfinal['Descricao'].astype('string')
    # dfxpfinal['Valor']=pd.to_numeric(dfxpfinal['Valor'], errors='coerce', downcast='float')

    # Apagar as linhas com dados ausentes
    dfxpfinal.dropna(inplace=True)

    return dfxpfinal


dfxp = {}
pdffile = 'D:\Flavio GDrive\Flavio Cloud\Projetos\PycharmProjects\TesteConverteOFX\input\XP_Visa_Infinite_2023-02-15.pdf'
csvfile = 'D:\Flavio GDrive\Flavio Cloud\Projetos\PycharmProjects\TesteConverteOFX\output\XP_Visa_Infinite_2023-02-15.csv'
ofxfile = 'D:\Flavio GDrive\Flavio Cloud\Projetos\PycharmProjects\TesteConverteOFX\output\XP_Visa_Infinite_2023-02-15.ofx'

# Leitura do arquivo no formato PDF
tables = camelot.read_pdf(pdffile, password='11272', pages='1,2,3', flavor='stream')

# Transforma em um Data Frame
dfxp[0] = tables[0].df
dfxp[1] = tables[1].df

# Data atual
# CurrentDateTime = datetime.datetime.now()
# date = CurrentDateTime.date()
# year = date.strftime("%Y")

ofx_str = convert_to_ofx_teste()

# dfxpfinal = tratamento_dfxp(dfxp, year)

# dfxpfinal.to_csv(csvfile, index=False)


#records = read_csv(csvfile, has_header=True)
#ofx_str = convert_to_ofx(records)

with open(ofxfile, "w") as output:
    output.write(ofx_str)
