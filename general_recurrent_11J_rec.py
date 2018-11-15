#! /usr/bin/python

import PyPDF2
import pandas as pd
import sys

from tabula import read_pdf 


def load_reader(file_path):
	# Import the the pdf document and return pdf Reader 
	try:
		pdfFileObj = open(file_path, 'rb')
		pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
		print('[O.K.]\n')
		return pdfReader 
	except:
		print('[Not O.K.]\n')
		return False


def write_pdf(pdfReader, file_path, page):
	try:
		# Initialise new writer
		pdfWriter = PyPDF2.PdfFileWriter()

		# Write a single page pdf
		pdfWriter.addPage(pdfReader.getPage(page))

		with open(file_path + '.pdf', 'wb') as outfile:
			pdfWriter.write(outfile)
		print('Saving...[O.K.]')
	except:
		print('Saving...[Not O.K.]')


def process_pdf(file_path, section, col_list):
	# Try to process with tabula with a single page
	try:
		# Read the pdf into a dataframe using tabula
		print('Processing...[O.K.]\n')
		return read_pdf(file_path + '.pdf',
						guess=False,
						columns=col_list,
						java_options="-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider",
						pandas_options={'header': None})
	
	except:
		# Try to process with tabula using multuiple tables
		try:
			# Read the pdf into a list of dataframes using tabula
			df_list = read_pdf(file_path + '.pdf', multiple_tables=True,
								guess=False,
								columns=col_list,
								java_options="-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider",
								pandas_options={'header': None})
			df = pd.concat(df_list)
			return df
			print('Processing...[O.K.]\n')
			
		except:
			print('Unable to process ' + str(page + 1))
			print('Processing...[Not O.K.]\n')


def process_df(dframe, section):
	if section == 'Section 1':
		
		df = dframe[(dframe['location_id'] != 'Section 1 - Lo')
					& (dframe['location_id'] != '2018 Funding A')
					& (dframe['location_id'] != 'A list of loca')
					& (dframe['location_id'] != 'Remote Level d')
					& (dframe['location_id'] != 'Reported')
					& (dframe['location_id'] != 'Delivery')
					& (dframe['location_id'] != 'Location ID')
					& (dframe['location_id'].notna())]
		
	elif section == 'Section 2':
		
		df = dframe[(dframe['subject_id'] != 'Section 2 - I')
					& (dframe['subject_id'] != '2018 Funding')
					& (dframe['subject_id'] != 'Notes: Unfund')
					& (dframe['subject_id'] != 'NON-AS')
					& (dframe['subject_id'] != 'Industry 18=0')
					& (dframe['subject_id'] != 'Program/')
					& (dframe['subject_id'] != 'Subject Id')
					& (dframe['subject_id'] != 'industry')
					& (dframe['subject_id'] != '(continued)')
					& (dframe['subject_id'] != 'Industry 18=1')]

	return df

if __name__ == '__main__':
	

	############################################################
	# Initialisation
	############################################################
	# Initialise page storage lists
	section_1_df_list = []
	section_2_df_list = []

	# Specify the pdf file location
	pdf_file_loc = 'general_recurrent_11K_Report_v2.pdf'

	# Determine the file path of where the output pdf will be stored
	file_stem = './single_output/output_page_'


	############################################################
	# Load pdf Report
	############################################################
	# Load the pdf document and return reader
	print('Loading pdf report...')
	pdfReader = load_reader(pdf_file_loc)
	if pdfReader == False:
		sys.exit()


	############################################################
	# Build files
	############################################################
	# Determine the number of pages of the report to scan through
	num_pages = pdfReader.numPages

	# Cycle through the pdf and build df
	for page in range(num_pages):
		
		# Extract the first part of the text from the page 
		section = pdfReader.getPage(page).extractText()[0:9]

		# Check if the current page is part of Section 1
		if section == 'Section 1' or section == 'Section 2':
			# Provide message about finding a Section 1 page
			print('Page ' + str(page+1) + ' is part of ' + section)
			
			# Specify file path to store single pdf
			file_path = file_stem + str(page+1)

			# Write a single page pdf for tabula to use
			write_pdf(pdfReader, file_path, page)

			# Save the returned pandas dataframe to a list
			if section == 'Section 1':
				col_list = [56, 158, 260, 421, 477]
				df = process_pdf(file_path, section, col_list)
				section_1_df_list.append(df)
			
			elif section == 'Section 2':
				# Process file and add to correct df
				col_list = [53.0,212.0,245.0,286.0,325.0,360.0,
							395.0,406.0,438.0,472.0,506.0,538.0,
							571.0,604.0,637.0,670.0,708.0]
				df = process_pdf(file_path, section, col_list)
				section_2_df_list.append(df)


	# Concatenate the final list of dataframes and save to csv
	df_s1 = pd.concat(section_1_df_list)
	df_s2 = pd.concat(section_2_df_list)

	# Attach headings to the pandas dataframe
	df_s1.columns = ['location_id',
					'location_name',
					'location_suburb',
					'dpt_location_name',
					'assigned_region',
					'remote_level']

	df_s2.columns = ['subject_id',
					'subject_name',
					'urban_ahc',
					'regional_ahc',
					'remote_ahc',
					'commenced_prior_2018',
					'unfindable_ahc',
					'null_col',
					'pass',
					'fail',
					'wthdrwl',
					'RPL Granted',
					'RPL Not Granted',
					'CT',
					'Trnsfrd',
					'Cont',
					'Not Started',
					'non_assess']

	# Process the concatenated dataframes
	df_s1 = process_df(df_s1, 'Section 1') # process df_s1
	df_s2 = process_df(df_s2, 'Section 2') # process df_s2

	df_s1.to_csv('section_1_output.csv', index=False, header=True)
	df_s2.to_csv('section_2_output.csv', index=False, header=True)
	