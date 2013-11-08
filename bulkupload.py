#
#  Purpose: Fill SNP postgres database from vcf output.
#			Run interactively as script prompts for user input
#			in case of repeated attempts to load the same file.
#
#-------------------------------------------------------------------------------------------
import psycopg2
import sys
import cyvcf
import re


# Connects to the database and finds author_id.
# If author is not present in the database, it is sent to insertAuthorID to be added.
def get_author_id(first_name, last_name):
    try:
        #Connecting to the database.
        dbh = psycopg2.connect(host='ngsdb', database='marea01', user='marea', password='marea')
        cur = dbh.cursor()
        try:
            # Checks to determine if the author already exists in the database. If yes, the author_id is returned.
            # If the author doesn't exist then it is inserted into the table.
            cur.execute('SELECT author_id FROM "ngsdbview_author" WHERE firstname = %s AND lastname = %s',
                        (first_name, last_name,))
            author_id = cur.fetchone()
            if author_id is not None:
                return author_id[0]
            else:
                author = insert_author_id(first_name, last_name)
                dbh.close()
                return author
        except psycopg2.DatabaseError, e:
            #If an error occurs during the SELECT, the database will roll back any possible changes to the database.
            print 'Error %s' % e
            sys.exit(1)
    #If database cannot be loaded, print error message.
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Inserts author into database. Prompts the user for a designation.
def insert_author_id(first_name, last_name):
    try:
        #Connecting to the database.
        dbh = psycopg2.connect(host='ngsdb', database='marea01', user='marea', password='marea')
        cur = dbh.cursor()
        designation = input("Please provide the designation for " + first_name + " " + last_name +
                            ". Most designations are the authors first and last initial.")
        email = input("Please provide the email for " + first_name + " " + last_name)
        try:
            cur.execute('INSERT INTO "ngsdbview_author" (firstname, lastname, designation, email) VALUES (%s, %s, %s, %s) RETURNING author_id',
                        (first_name, last_name, designation, email))
            author = cur.fetchone[0]
        except psycopg2.DatabaseError, e:
            #If an error occurs, the database will roll back any possible changes to the database.
            if dbh:
                dbh.rollback()
                print 'Error %s' % e
                sys.exit(1)
    #If database cannot be loaded, print error message.
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    dbh.commit()
    dbh.close()
    return author


# def insertAnalysisType(analysisType):
#     for each in analysisType:
#         analysisInfo = each.split()
#         analysis_type = analysisInfo[0].split('=')[1]
#         definition = input("Please provide a definition for the following analysis type: " + analysis_type)
#         try:
#             dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
#             cur = dbh.cursor()
#             #Inserts data into the database through 'execute'. Requires a tuple of values.
#             cur.execute('INSERT INTO "ngsdbview_analysistype" (analysis_type, definition) VALUES (%s, %s)',
#                         (analysis_type, definition))
#             dbh.commit()
#             dbh.close()
#         except psycopg2.DatabaseError, e:
#             print 'Error %s' % e
#             sys.exit(1)
#         finally:
#             if dbh:
#                 dbh.close()


# Inserts statistic cvs from the INFO metadata into statistics cv when they are not already present.
# This information will be pulled to fill in the statistics table.
def insert_statistics_cv(infos, formts):
    for each in infos:
        try:
            cv_name = each
            print each
            print dict.get(each)
            #vcf_reader = cyvcf.Reader(open('/Volumes/mcobb$/Ld06_v01s1.vcf.gz.snpEff.vcf', 'r'))
            #cv_definition = vcf_reader.infos[each][3]
            cvgroup_id = 1
            dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
            cur = dbh.cursor()
            try:
                cur.execute('SELECT cvterm_id FROM "ngsdbview_statistics_cv" WHERE cvgroup_id = %s AND cvterm = %s',
                            (cvgroup_id, cv_name,))
                cv_id = cur.fetchone()
                if cv_id is not None:
                    dbh.commit()
                    dbh.close()
                    return cv_id[0]
                #Inserts data into the database through 'executemany'.
                else:
                    cur.execute('INSERT INTO "ngsdbview_statistics_cv" (cvgroup_id, cvterm, cv_notes) VALUES (%s, %s, %s) RETURNING cvterm_id',
                                (cvgroup_id,  cv_name, cv_definition))
                    cvterm_id = cur.fetchone()[0]
                    dbh.commit()
                    dbh.close()
                    return cvterm_id
            except psycopg2.IntegrityError:
                dbh.rollback()
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)


# Inserts the possible snp types into snp_type if they do not already exist.
# This information will be pulled for the snp_result table.
def insert_snp_type(indel, deletion, is_snp, monomorphic, transition, sv):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('INSERT INTO "ngsdbview_snp_type" (indel, deletion, is_snp, monomorphic, transition, sv) VALUES (%s, %s, %s, %s, %s, %s) RETURNING snptype_id',
                        (indel, deletion, is_snp, monomorphic, transition, sv))
            snptype_id = cur.fetchone()[0]
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    dbh.commit()
    dbh.close()
    return snptype_id


# This table inserts each type of snp for the snp_result table. The snp_id is pulled from the snp_type table.
def insert_snp_results(result_id, ref_base, alt_base, heterozygosity, quality, sample_id, chromosome_id, snp_type):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('INSERT INTO  "SNP" (result_id, ref_base, alt_base, heterozygosity, quality, sample_id, chromosome_id, snp_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,) RETURNING snp_id',
                        (result_id, ref_base, alt_base, heterozygosity, quality, sample_id, chromosome_id, snp_type))
            snp_id = cur.fetchone()[0]
        except psycopg2.IntegrityError:
            dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    dbh.commit()
    dbh.close()
    return snp_id


# Inserts types of snp effects into effect_cv if they are not already listed.
# Effects are collected from the INFO metadata where id=EFF.
# Effect id will be pulled to fill in the effect table.
def insert_effect_cv(effect_list):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        effects = effect_list.split(':')[1]
        effect = re.split('\( | \)', effects)[1]
        effect_string = re.split('\[', effect)[0]
        for each in effect_string:
            try:
                cur.execute('SELECT effect_id FROM "ngsdbview_effect_cv" WHERE effect_string = %s', each)
                effect_id = cur.fetchone()[0]
                if effect_id is not None:
                    dbh.close()
                    return effect_id
                else:
                    cur.execute('INSERT INTO "ngsdbview_effect_cv" (effect) VALUES (%s) RETURNING effect_id', each)
                    effect_id = cur.fetchone()[0]
                    dbh.commit()
                    dbh.close()
                    return effect_id
            except psycopg2.IntegrityError:
                dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# Identifies the chromosome ID if in the database. If not, the chromosome is added to the chromosome table.
def insert_chromosome(chromosome_name, organism_id):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        for each in chromosome_name:
            chromosome_fullname = each[0]
            size = each[1]
            chromosome = chromosome_fullname.split('_')[0]
            version = chromosome_fullname.split('_')[1]
            try:
                cur.execute('SELECT chromosome_id FROM "ngsdbview_chromosome" WHERE chromosome_name = %s AND chromosome_version = %s',
                            (chromosome, version))
                chromosome_id = cur.fetchone()
                if chromosome_id is not None:
                    dbh.commit()
                    dbh.close()
                    return chromosome_id[0]
                else:
                    cur.execute('INSERT INTO "ngsdbview_chromosome" (chromosome_name, chromosome_version, size, organism_id) VALUES (%s, %s, %s, %s) RETURNING chromosome_id',
                                (chromosome, version, size, organism_id))
                    chromosome = cur.fetchone()[0]
                    dbh.commit()
                    dbh.close()
                    return chromosome
            except psycopg2.IntegrityError:
                dbh.rollback()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_organism_id(organism):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT organism_id FROM "ngsdbview_organism" WHERE organismcode = %s',
                        (organism,))
            organism_id = cur.fetchone()
            if organism_id is not None:
                return organism_id[0]
            else:
                print "Please add organism to the ngsdb database."

        except psycopg2.IntegrityError:
            dbh.rollback()

        dbh.commit()
        dbh.close()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_chromosome_id(chromosome):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            chromosome_name = chromosome.split('_')[0]
            version = chromosome.split('_')[1]
            cur.execute('SELECT chromosome_id FROM "ngsdbview_chromosome" WHERE chromosome_name = %s AND chromosome_version = %s', (chromosome_name, version))
            chromosome_id = cur.fetchone()
            if chromosome_id is not None:
                return chromosome_id[0]
            else:
                print "Please add the chromosome to the ngsdb database."
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_genome_id(organism, version):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT genome_id FROM "ngsdbview_genome" WHERE organism_id = %s AND version = %s', (organism, version))
            genome_id = cur.fetchone()
            if genome_id is not None:
                return genome_id[0]
            else:
                print "Please add the genome to the ngsdb database."
        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_result(genome_id, author_id, analysisPath):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        try:
            cur.execute('SELECT result_id FROM "ngsdbview_result"  WHERE genome_id = %s AND author_id = %s', (genome_id, author_id))
            result = cur.fetchone()
            if result is not None:
                cur.execute('SELECT library_id FROM "ngsdbview_result_libraries" WHERE result_id = %s', result)
                library_id = cur.fetchone()
                print "Duplicate library exists. Please manually add the results."
                return library_id
            else:
                insert_result(genome_id, author_id, analysisPath)

        except psycopg2.IntegrityError:
            dbh.rollback()
        dbh.commit()
        dbh.close()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def insert_result(genome_id, author_id, analysisPath):
    try:
        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
        cur = dbh.cursor()
        is_current = True
        is_obsolete = False
        notes = ''
        cur.execute('INSERT INTO "ngsdbview_result" (genome_id, author_id, is_current, is_obsolete, analysisPath, notes) VALUES (%s, %s, %s, %s, %s, %s) RETURNING result_id',
                    (genome_id, author_id, is_current, is_obsolete, analysisPath, notes))
        result_id = cur.fetchone()[0]
        dbh.commit()
        dbh.close()
        return result_id
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def get_heterozygosity(samples):
    for call in samples:
        heterozygosity = call.is_het
        return heterozygosity


#def get_library_id(result_id):
#    try:
#        dbh = psycopg2.connect(host='ngsdb', database="marea01", user='marea', password='marea')
#        cur = dbh.cursor()
#        cur.execute('SELECT library_id FROM "ngsdbview_result_libraries" WHERE result_id = %s', [result_id])
#        library_id = cur.fetchone()
#        if library_id is not None:
#            return library_id[0]
#        else:
#            cur.execute('INSERT INTO "ngsdbview_result" (genome_id, author_id, is_current, is_o
#            dbh.commit()
#            dbh.close()
#    except psycopg2.DatabaseError, e:
#        print 'Error %s' % e
#        sys.exit(1)

def main():

    number_of_files = input("Type 'yes' if you have both the vcf file and summary file. "
                            "Type 'no' if you only have the vcf file.")

    if number_of_files == "no":
        print "Please note that without a summary file, the snp_summary & summary_level_cv table will not be updated."\
              "This can be done manually at a later date"
        vcf_file = input("Please provide the full path of the vcf file.")
        # collect and import vcf file.
        vcf_reader = cyvcf.Reader(open('/Volumes/mcobb$/Ld06_v01s1.vcf.gz.snpEff.vcf', 'r'))
        record = vcf_reader.next()

        ## Identifies the organism and strain.
        #organism = input("Please state the organism name. If you are unsure of the correct name, please visit the " +
        #                  "NGSDB database at dummy.com")
        #organism_id = get_organism_id(organism)
        #
        #genome_version = input("Please state the genome version")
        #genome_id = getGenomeID(organism_id, genome_version)
        #
        ## # Identifies the chromosome and chromosomal version
        ## chromosome_name = cyvcf_reader.contigs.values()
        ## chromosome_id =insert_chromosome(chromosome_name, organism_id)
        #
        #analysisPath = input('Please provide the full analysis path.')

        ## Identifies the author id. If the author is not in the database they are added.
        #if cyvcf_reader.metadata['SnpEffVersion']:
        #     firstname = vcf_reader.metadata['SnpEffVersion'][0].split()[4]
        #     lastname = vcf_reader.metadata['SnpEffVersion'][0].split()[5].strip('"')
        #     author_id = get_author_id(firstname, lastname)
        #else:
        #     fullname = input("Please provide the authors first and last name")
        #     firstname = fullname.split()[0]
        #     lastname = fullname.split()[1]
        #     author_id = get_author_id(firstname, lastname)

        # Inserts Statistic CVs into the Statistics_CV.
        info = vcf_reader.infos
        #print info
        formats = vcf_reader.formats
        #print formats
        insert_statistics_cv(info, formats)

        # Inserts Effect types into effect_cv
        # effect_list = vcf_reader.infos['EFF'].desc
        # insert_effect_cv(effect_list)

        # Inserts Results into file
        #result_id = insert_result(genome_id, author_id, analysisPath)

        #Find author_id or create new author_id if new author.
        for each in vcf_reader:
            ref_base = record.REF
            alt_base = record.ALT
            quality = record.QUAL
            filter = record.FILTER
            position = record.POS
            format = record.FORMAT
            is_snp = record.is_snp
            indel = record.is_indel
            deletion = record.is_deletion
            monomorphic = record.is_monomorphic
            #structural variant
            sv = record.is_sv
            transition = record.is_transition


            # finds chromosome_id
            # chromosome = record.CHROM
            # get_chromosome_id(chromosome)


            # Inserts SNP statistics into Statistics
            # insert_statistics(snp_id, statistics_cvterm_id)

            # Inserts the SNP types.
            # snptype_id = insert_snp_type(indel, deletion, is_snp, monomorphic, transition, sv)

            # Need library_id
            #samples = each.samples
            #heterozygosity = get_heterozygosity(samples)

            #result_id = 2
            #library_id = get_library_id(result_id)
            #print library_id


            # Need library_id
            #insert_snp_results(result_id, ref_base, alt_base, heterozygosity, quality, library_id, chromosome_id, snptype_id)

            # Inserts effects on each SNP into Effect. Need SNP_id
            # add_snp_effect()

    elif number_of_files == "yes":
        vcf_file = input("Please provide the full path of the vcf file.")
        sumamry_file = input("Please provide the full path of the summary file")

        # collect and import vcf file.
        vcf_reader = cyvcf.Reader(open('/Volumes/mcobb$/Ld06_v01s1.vcf.gz.snpEff.vcf', 'r'))
        record = vcf_reader.next()

    else:
        print "A vcf file is required for this program. Please try again."

main()


    #----------------------------------------------------------------------------------

            # Identifies the reference genome
            # genomeSource = cyvcf_reader.metadata['reference']
            # print genomeSource

            # Identifies the analysis type and returns analysis_id
            # analysisType = cyvcf_reader.metadata['UnifiedGenotyper']
            # insertAnalysisType(analysisType)

            # Get sample number from library_id.
            # library =








    #----------------------------------------------------------------------------------
    # Code snippets
    #
    #
    #		cur.execute("SELECT * FROM author")
    #		rows = cur.fetchall()
    #		for row in rows:
    #			print row
    #
    #
