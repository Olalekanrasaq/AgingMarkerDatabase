import pandas as pd
import re

def parse_subject(csv_file):
    '''A function that convert all unkown values in the data into None in python'''
    missing_values = ["unknown", "Unknown"]
    # read the csv file and set the parameter for na_values to the missing values
    subject = pd.read_csv(csv_file, na_values=missing_values) 
    return subject

def parse_proteome(tsv_file):
    prot_abd = pd.read_csv(tsv_file, sep='\t')
    # split the sample ID into two seperate components which are Sample ID and Visist ID
    prot_abd[['sampleID','VisitID']] = prot_abd.SampleID.apply(lambda x: pd.Series(str(x).split("-")))
    prot_abd = prot_abd[['sampleID','VisitID']]
    return prot_abd

def parse_transcriptome(tsv_file):
    tran_abd = pd.read_csv(tsv_file, sep='\t')
    tran_abd[['sampleID','VisitID']] = tran_abd.SampleID.apply(lambda x: pd.Series(str(x).split("-")))
    tran_abd = tran_abd[['sampleID', 'VisitID', 'A1BG']]
    return tran_abd
    
def parse_metabolome(tsv_file):
    met_abd = pd.read_csv(tsv_file, sep='\t')
    met_abd[['sampleID','VisitID']] = met_abd.SampleID.apply(lambda x: pd.Series(str(x).split("-")))
    met_abd = met_abd[['sampleID','VisitID']]  
    return met_abd

def parse_annotation(csv_file):
    met_annot = pd.read_csv(csv_file)
    cols = ['Metabolite', 'KEGG', 'HMDB']
    met_annot_exp = pd.concat([met_annot[col].str.split('|', expand=True) for col in cols], 
                 axis=1, keys=cols)\
         .stack()\
         .reset_index(level=1, drop=True)\
         .join(met_annot[['PeakID','Chemical Class', 'Pathway']])\
         .reset_index(drop=True)
    first_col = met_annot_exp.pop('PeakID')
    met_annot_exp.insert(0, 'PeakID', first_col)
    met_annot_exp['Metabolite'] = met_annot_exp['Metabolite'].str.replace(r'\(\d{1}\)', '')
    return met_annot_exp
  