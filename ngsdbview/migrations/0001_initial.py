# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'Author'
        db.create_table(u'ngsdbview_author', (
            ('author_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('firstname', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('lastname', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('designation', self.gf('django.db.models.fields.CharField')(unique=True, max_length=5)),
            ('email', self.gf('django.db.models.fields.EmailField')(unique=True, max_length=75)),
        ))
        db.send_create_signal(u'ngsdbview', ['Author'])

        # Adding model 'Librarytype'
        db.create_table(u'ngsdbview_librarytype', (
            ('librarytype_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('type', self.gf('django.db.models.fields.CharField')(unique=True, max_length=25)),
            ('notes', self.gf('django.db.models.fields.TextField')(max_length=400, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Librarytype'])

        # Adding model 'Protocol'
        db.create_table(u'ngsdbview_protocol', (
            ('protocol_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(unique=True, max_length=50)),
            ('sopfile', self.gf('django.db.models.fields.files.FileField')(max_length=100)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=400)),
        ))
        db.send_create_signal(u'ngsdbview', ['Protocol'])

        # Adding model 'Seqtech'
        db.create_table(u'ngsdbview_seqtech', (
            ('seqtech_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(unique=True, max_length=100)),
            ('notes', self.gf('django.db.models.fields.TextField')(blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Seqtech'])

        # Adding model 'Lifestage'
        db.create_table(u'ngsdbview_lifestage', (
            ('lifestage_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('lifestage', self.gf('django.db.models.fields.CharField')(unique=True, max_length=45)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=400)),
        ))
        db.send_create_signal(u'ngsdbview', ['Lifestage'])

        # Adding model 'Phenotype'
        db.create_table(u'ngsdbview_phenotype', (
            ('phenotype_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('phenotype', self.gf('django.db.models.fields.CharField')(unique=True, max_length=45)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=45)),
        ))
        db.send_create_signal(u'ngsdbview', ['Phenotype'])

        # Adding model 'Growthphase'
        db.create_table(u'ngsdbview_growthphase', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('growthphase', self.gf('django.db.models.fields.CharField')(unique=True, max_length=100)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=400, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Growthphase'])

        # Adding model 'Genotype'
        db.create_table(u'ngsdbview_genotype', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('genotype', self.gf('django.db.models.fields.CharField')(unique=True, max_length=45)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=45, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Genotype'])

        # Adding model 'Collaborator'
        db.create_table(u'ngsdbview_collaborator', (
            ('collaborator_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('firstname', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('lastname', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('email', self.gf('django.db.models.fields.EmailField')(unique=True, max_length=100)),
            ('affiliation', self.gf('django.db.models.fields.CharField')(max_length=100)),
            ('sharepoint_site', self.gf('django.db.models.fields.URLField')(max_length=200, blank=True)),
            ('ftp_path', self.gf('django.db.models.fields.URLField')(max_length=200, blank=True)),
            ('ftp_username', self.gf('django.db.models.fields.CharField')(max_length=50, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Collaborator'])

        # Adding model 'Organism'
        db.create_table(u'ngsdbview_organism', (
            ('organism_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('organismcode', self.gf('django.db.models.fields.CharField')(unique=True, max_length=10)),
            ('genus', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('speceis', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('strain', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('isolate', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('source', self.gf('django.db.models.fields.CharField')(max_length=100)),
        ))
        db.send_create_signal(u'ngsdbview', ['Organism'])

        # Adding model 'Software'
        db.create_table(u'ngsdbview_software', (
            ('software_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('version', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('algorithm', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('source', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('sourceuri', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('notes', self.gf('django.db.models.fields.TextField')(default=None)),
        ))
        db.send_create_signal(u'ngsdbview', ['Software'])

        # Adding model 'Analysistype'
        db.create_table(u'ngsdbview_analysistype', (
            ('analysistype_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('type', self.gf('django.db.models.fields.CharField')(unique=True, max_length=255)),
            ('definition', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Analysistype'])

        # Adding model 'Dbxref'
        db.create_table(u'ngsdbview_dbxref', (
            ('dbxref_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(unique=True, max_length=255)),
            ('url', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('definition', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Dbxref'])

        # Adding model 'Cv'
        db.create_table(u'ngsdbview_cv', (
            ('cv_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(unique=True, max_length=255, db_index=True)),
            ('definition', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Cv'])

        # Adding model 'Cvterm'
        db.create_table(u'ngsdbview_cvterm', (
            ('cvterm_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('cv', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Cv'])),
            ('name', self.gf('django.db.models.fields.CharField')(max_length=1024, db_index=True)),
            ('definition', self.gf('django.db.models.fields.TextField')()),
            ('dbxref', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Dbxref'])),
            ('is_obsolete', self.gf('django.db.models.fields.BooleanField')(default=True)),
            ('is_relationshiptype', self.gf('django.db.models.fields.BooleanField')(default=False)),
        ))
        db.send_create_signal(u'ngsdbview', ['Cvterm'])

        # Adding model 'Genome'
        db.create_table(u'ngsdbview_genome', (
            ('genome_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('organism', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Organism'])),
            ('version', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('source', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('svnrevision', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('svnpath', self.gf('django.db.models.fields.TextField')(blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Genome'])

        # Adding model 'Library'
        db.create_table(u'ngsdbview_library', (
            ('library_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('librarycode', self.gf('django.db.models.fields.CharField')(unique=True, max_length=25, db_index=True)),
            ('author', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Author'])),
            ('organism', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Organism'])),
            ('lifestage', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Lifestage'])),
            ('phenotype', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Phenotype'])),
            ('collaborator', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Collaborator'])),
            ('librarytype', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Librarytype'])),
            ('protocol', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Protocol'])),
            ('fastqname', self.gf('django.db.models.fields.CharField')(max_length=1000)),
            ('fastqalias', self.gf('django.db.models.fields.CharField')(max_length=1000)),
            ('librarysize', self.gf('django.db.models.fields.IntegerField')()),
            ('flowcell', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('downloaddate', self.gf('django.db.models.fields.DateField')()),
            ('notes', self.gf('django.db.models.fields.CharField')(max_length=400)),
            ('fastqpath', self.gf('django.db.models.fields.CharField')(max_length=1025)),
        ))
        db.send_create_signal(u'ngsdbview', ['Library'])

        # Adding model 'Libraryfile'
        db.create_table(u'ngsdbview_libraryfile', (
            ('libraryfile_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('library', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Library'])),
            ('notes', self.gf('django.db.models.fields.CharField')(default='qc', max_length=1000)),
            ('file', self.gf('django.db.models.fields.files.FileField')(max_length=100)),
        ))
        db.send_create_signal(u'ngsdbview', ['Libraryfile'])

        # Adding model 'Libraryprop'
        db.create_table(u'ngsdbview_libraryprop', (
            ('libraryprop_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('library', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Library'])),
            ('cvterm', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Cvterm'])),
            ('value', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Libraryprop'])

        # Adding model 'Result'
        db.create_table(u'ngsdbview_result', (
            ('result_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('genome', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Genome'])),
            ('author', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Author'])),
            ('is_current', self.gf('django.db.models.fields.BooleanField')(default=True)),
            ('is_obsolete', self.gf('django.db.models.fields.BooleanField')(default=True)),
            ('analysispath', self.gf('django.db.models.fields.CharField')(max_length=255)),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
            ('notes', self.gf('django.db.models.fields.TextField')(default=None)),
        ))
        db.send_create_signal(u'ngsdbview', ['Result'])

        # Adding M2M table for field libraries on 'Result'
        m2m_table_name = db.shorten_name(u'ngsdbview_result_libraries')
        db.create_table(m2m_table_name, (
            ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True)),
            ('result', models.ForeignKey(orm[u'ngsdbview.result'], null=False)),
            ('library', models.ForeignKey(orm[u'ngsdbview.library'], null=False))
        ))
        db.create_unique(m2m_table_name, ['result_id', 'library_id'])

        # Adding model 'Resultfile'
        db.create_table(u'ngsdbview_resultfile', (
            ('resultfile_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('notes', self.gf('django.db.models.fields.CharField')(default='result', max_length=1000)),
            ('file', self.gf('django.db.models.fields.files.FileField')(max_length=100)),
        ))
        db.send_create_signal(u'ngsdbview', ['Resultfile'])

        # Adding model 'Resultprop'
        db.create_table(u'ngsdbview_resultprop', (
            ('resultprop_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('cvterm', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Cvterm'])),
            ('value', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Resultprop'])

        # Adding model 'Resultraw'
        db.create_table(u'ngsdbview_resultraw', (
            ('resultraw_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('chromosome', self.gf('django.db.models.fields.CharField')(max_length=250, db_index=True)),
            ('position', self.gf('django.db.models.fields.IntegerField')(db_index=True)),
            ('totalcount', self.gf('django.db.models.fields.IntegerField')()),
            ('posstrandcount', self.gf('django.db.models.fields.IntegerField')()),
            ('negstrandcount', self.gf('django.db.models.fields.IntegerField')()),
            ('majorstrand', self.gf('django.db.models.fields.IntegerField')()),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Resultraw'])

        # Adding model 'Resultslsite'
        db.create_table(u'ngsdbview_resultslsite', (
            ('resultslsite_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('slid', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('chromosome', self.gf('django.db.models.fields.CharField')(max_length=15, db_index=True)),
            ('position', self.gf('django.db.models.fields.IntegerField')(db_index=True)),
            ('readcount', self.gf('django.db.models.fields.IntegerField')()),
            ('readstrand', self.gf('django.db.models.fields.CharField')(max_length=5)),
            ('geneid', self.gf('django.db.models.fields.CharField')(max_length=45, db_index=True)),
            ('genestrand', self.gf('django.db.models.fields.IntegerField')()),
            ('cdsstart', self.gf('django.db.models.fields.IntegerField')()),
            ('cdsend', self.gf('django.db.models.fields.IntegerField')()),
            ('putative5utr', self.gf('django.db.models.fields.IntegerField')()),
            ('rank', self.gf('django.db.models.fields.FloatField')(db_index=True)),
            ('intervallength', self.gf('django.db.models.fields.IntegerField')()),
            ('slpercent', self.gf('django.db.models.fields.DecimalField')(max_digits=10, decimal_places=7)),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Resultslsite'])

        # Adding model 'Resultslgene'
        db.create_table(u'ngsdbview_resultslgene', (
            ('resultslgene_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('geneid', self.gf('django.db.models.fields.CharField')(max_length=45, db_index=True)),
            ('genestrand', self.gf('django.db.models.fields.IntegerField')()),
            ('chromosome', self.gf('django.db.models.fields.CharField')(max_length=25)),
            ('cdsstart', self.gf('django.db.models.fields.IntegerField')()),
            ('cdsend', self.gf('django.db.models.fields.IntegerField')()),
            ('putative5utr', self.gf('django.db.models.fields.IntegerField')()),
            ('sensesitecount', self.gf('django.db.models.fields.IntegerField')()),
            ('antisensesitecount', self.gf('django.db.models.fields.IntegerField')()),
            ('sensereadcount', self.gf('django.db.models.fields.IntegerField')()),
            ('antisensereadcount', self.gf('django.db.models.fields.IntegerField')()),
            ('senseupsitecount', self.gf('django.db.models.fields.IntegerField')()),
            ('sensedownsitecount', self.gf('django.db.models.fields.IntegerField')()),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Resultslgene'])

        # Adding model 'Analysis'
        db.create_table(u'ngsdbview_analysis', (
            ('analysis_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('analysistype', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Analysistype'])),
            ('software', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Software'])),
            ('result', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Result'])),
            ('ordinal', self.gf('django.db.models.fields.IntegerField')()),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
            ('notes', self.gf('django.db.models.fields.TextField')(null=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Analysis'])

        # Adding model 'Analysisfile'
        db.create_table(u'ngsdbview_analysisfile', (
            ('analysisfile_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('analysis', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Analysis'])),
            ('notes', self.gf('django.db.models.fields.CharField')(default='analysis', max_length=1000)),
            ('file', self.gf('django.db.models.fields.files.FileField')(max_length=100)),
        ))
        db.send_create_signal(u'ngsdbview', ['Analysisfile'])

        # Adding model 'Analysisprop'
        db.create_table(u'ngsdbview_analysisprop', (
            ('analysisprop_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('analysis', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Analysis'])),
            ('cvterm', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Cvterm'])),
            ('value', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'ngsdbview', ['Analysisprop'])

        # Adding model 'Feature'
        db.create_table(u'ngsdbview_feature', (
            ('feature_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('featuretype', self.gf('django.db.models.fields.CharField')(max_length=40)),
            ('genome', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['ngsdbview.Genome'])),
            ('chromosome', self.gf('django.db.models.fields.CharField')(max_length=45, db_index=True)),
            ('geneid', self.gf('django.db.models.fields.CharField')(max_length=100, db_index=True)),
            ('fmin', self.gf('django.db.models.fields.IntegerField')()),
            ('fmax', self.gf('django.db.models.fields.IntegerField')()),
            ('strand', self.gf('django.db.models.fields.IntegerField')()),
            ('phase', self.gf('django.db.models.fields.CharField')(max_length=5)),
            ('geneproduct', self.gf('django.db.models.fields.CharField')(max_length=500, db_index=True)),
            ('annotation', self.gf('django.db.models.fields.CharField')(max_length=500, db_index=True)),
            ('na_seq', self.gf('django.db.models.fields.TextField')()),
            ('aa_seq', self.gf('django.db.models.fields.TextField')()),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now_add=True, blank=True)),
            ('time_data_modified', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Feature'])

        # Adding model 'Geneidmap'
        db.create_table(u'ngsdbview_geneidmap', (
            ('geneidmap_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('geneid_current', self.gf('django.db.models.fields.CharField')(max_length=50, db_index=True)),
            ('geneid_previous', self.gf('django.db.models.fields.CharField')(max_length=50, db_index=True)),
            ('db_soruce_previous', self.gf('django.db.models.fields.CharField')(max_length=50)),
            ('db_version_previous', self.gf('django.db.models.fields.CharField')(max_length=50)),
            ('db_soruce_current', self.gf('django.db.models.fields.CharField')(max_length=50)),
            ('db_version_current', self.gf('django.db.models.fields.CharField')(max_length=50)),
            ('time_data_loaded', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['Geneidmap'])

        # Adding model 'UserProfile'
        db.create_table(u'ngsdbview_userprofile', (
            ('id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('user', self.gf('django.db.models.fields.related.OneToOneField')(to=orm['auth.User'], unique=True)),
        ))
        db.send_create_signal(u'ngsdbview', ['UserProfile'])

        # Adding M2M table for field libraries on 'UserProfile'
        m2m_table_name = db.shorten_name(u'ngsdbview_userprofile_libraries')
        db.create_table(m2m_table_name, (
            ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True)),
            ('userprofile', models.ForeignKey(orm[u'ngsdbview.userprofile'], null=False)),
            ('library', models.ForeignKey(orm[u'ngsdbview.library'], null=False))
        ))
        db.create_unique(m2m_table_name, ['userprofile_id', 'library_id'])


    def backwards(self, orm):
        # Deleting model 'Author'
        db.delete_table(u'ngsdbview_author')

        # Deleting model 'Librarytype'
        db.delete_table(u'ngsdbview_librarytype')

        # Deleting model 'Protocol'
        db.delete_table(u'ngsdbview_protocol')

        # Deleting model 'Seqtech'
        db.delete_table(u'ngsdbview_seqtech')

        # Deleting model 'Lifestage'
        db.delete_table(u'ngsdbview_lifestage')

        # Deleting model 'Phenotype'
        db.delete_table(u'ngsdbview_phenotype')

        # Deleting model 'Growthphase'
        db.delete_table(u'ngsdbview_growthphase')

        # Deleting model 'Genotype'
        db.delete_table(u'ngsdbview_genotype')

        # Deleting model 'Collaborator'
        db.delete_table(u'ngsdbview_collaborator')

        # Deleting model 'Organism'
        db.delete_table(u'ngsdbview_organism')

        # Deleting model 'Software'
        db.delete_table(u'ngsdbview_software')

        # Deleting model 'Analysistype'
        db.delete_table(u'ngsdbview_analysistype')

        # Deleting model 'Dbxref'
        db.delete_table(u'ngsdbview_dbxref')

        # Deleting model 'Cv'
        db.delete_table(u'ngsdbview_cv')

        # Deleting model 'Cvterm'
        db.delete_table(u'ngsdbview_cvterm')

        # Deleting model 'Genome'
        db.delete_table(u'ngsdbview_genome')

        # Deleting model 'Library'
        db.delete_table(u'ngsdbview_library')

        # Deleting model 'Libraryfile'
        db.delete_table(u'ngsdbview_libraryfile')

        # Deleting model 'Libraryprop'
        db.delete_table(u'ngsdbview_libraryprop')

        # Deleting model 'Result'
        db.delete_table(u'ngsdbview_result')

        # Removing M2M table for field libraries on 'Result'
        db.delete_table(db.shorten_name(u'ngsdbview_result_libraries'))

        # Deleting model 'Resultfile'
        db.delete_table(u'ngsdbview_resultfile')

        # Deleting model 'Resultprop'
        db.delete_table(u'ngsdbview_resultprop')

        # Deleting model 'Resultraw'
        db.delete_table(u'ngsdbview_resultraw')

        # Deleting model 'Resultslsite'
        db.delete_table(u'ngsdbview_resultslsite')

        # Deleting model 'Resultslgene'
        db.delete_table(u'ngsdbview_resultslgene')

        # Deleting model 'Analysis'
        db.delete_table(u'ngsdbview_analysis')

        # Deleting model 'Analysisfile'
        db.delete_table(u'ngsdbview_analysisfile')

        # Deleting model 'Analysisprop'
        db.delete_table(u'ngsdbview_analysisprop')

        # Deleting model 'Feature'
        db.delete_table(u'ngsdbview_feature')

        # Deleting model 'Geneidmap'
        db.delete_table(u'ngsdbview_geneidmap')

        # Deleting model 'UserProfile'
        db.delete_table(u'ngsdbview_userprofile')

        # Removing M2M table for field libraries on 'UserProfile'
        db.delete_table(db.shorten_name(u'ngsdbview_userprofile_libraries'))


    models = {
        u'auth.group': {
            'Meta': {'object_name': 'Group'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '80'}),
            'permissions': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['auth.Permission']", 'symmetrical': 'False', 'blank': 'True'})
        },
        u'auth.permission': {
            'Meta': {'ordering': "(u'content_type__app_label', u'content_type__model', u'codename')", 'unique_together': "((u'content_type', u'codename'),)", 'object_name': 'Permission'},
            'codename': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'content_type': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['contenttypes.ContentType']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '50'})
        },
        u'auth.user': {
            'Meta': {'object_name': 'User'},
            'date_joined': ('django.db.models.fields.DateTimeField', [], {'default': 'datetime.datetime.now'}),
            'email': ('django.db.models.fields.EmailField', [], {'max_length': '75', 'blank': 'True'}),
            'first_name': ('django.db.models.fields.CharField', [], {'max_length': '30', 'blank': 'True'}),
            'groups': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['auth.Group']", 'symmetrical': 'False', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'is_active': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'is_staff': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'is_superuser': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'last_login': ('django.db.models.fields.DateTimeField', [], {'default': 'datetime.datetime.now'}),
            'last_name': ('django.db.models.fields.CharField', [], {'max_length': '30', 'blank': 'True'}),
            'password': ('django.db.models.fields.CharField', [], {'max_length': '128'}),
            'user_permissions': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['auth.Permission']", 'symmetrical': 'False', 'blank': 'True'}),
            'username': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '30'})
        },
        u'contenttypes.contenttype': {
            'Meta': {'ordering': "('name',)", 'unique_together': "(('app_label', 'model'),)", 'object_name': 'ContentType', 'db_table': "'django_content_type'"},
            'app_label': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'model': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'ngsdbview.analysis': {
            'Meta': {'object_name': 'Analysis'},
            'analysis_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'analysistype': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Analysistype']"}),
            'notes': ('django.db.models.fields.TextField', [], {'null': 'True'}),
            'ordinal': ('django.db.models.fields.IntegerField', [], {}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'software': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Software']"}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.analysisfile': {
            'Meta': {'object_name': 'Analysisfile'},
            'analysis': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Analysis']"}),
            'analysisfile_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'file': ('django.db.models.fields.files.FileField', [], {'max_length': '100'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': "'analysis'", 'max_length': '1000'})
        },
        u'ngsdbview.analysisprop': {
            'Meta': {'object_name': 'Analysisprop'},
            'analysis': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Analysis']"}),
            'analysisprop_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'cvterm': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Cvterm']"}),
            'value': ('django.db.models.fields.TextField', [], {})
        },
        u'ngsdbview.analysistype': {
            'Meta': {'object_name': 'Analysistype'},
            'analysistype_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'definition': ('django.db.models.fields.TextField', [], {}),
            'type': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '255'})
        },
        u'ngsdbview.author': {
            'Meta': {'object_name': 'Author'},
            'author_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'designation': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '5'}),
            'email': ('django.db.models.fields.EmailField', [], {'unique': 'True', 'max_length': '75'}),
            'firstname': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'lastname': ('django.db.models.fields.CharField', [], {'max_length': '45'})
        },
        u'ngsdbview.collaborator': {
            'Meta': {'object_name': 'Collaborator'},
            'affiliation': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'collaborator_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'email': ('django.db.models.fields.EmailField', [], {'unique': 'True', 'max_length': '100'}),
            'firstname': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'ftp_path': ('django.db.models.fields.URLField', [], {'max_length': '200', 'blank': 'True'}),
            'ftp_username': ('django.db.models.fields.CharField', [], {'max_length': '50', 'blank': 'True'}),
            'lastname': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'sharepoint_site': ('django.db.models.fields.URLField', [], {'max_length': '200', 'blank': 'True'})
        },
        u'ngsdbview.cv': {
            'Meta': {'object_name': 'Cv'},
            'cv_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'definition': ('django.db.models.fields.TextField', [], {}),
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '255', 'db_index': 'True'})
        },
        u'ngsdbview.cvterm': {
            'Meta': {'object_name': 'Cvterm'},
            'cv': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Cv']"}),
            'cvterm_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'dbxref': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Dbxref']"}),
            'definition': ('django.db.models.fields.TextField', [], {}),
            'is_obsolete': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'is_relationshiptype': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '1024', 'db_index': 'True'})
        },
        u'ngsdbview.dbxref': {
            'Meta': {'object_name': 'Dbxref'},
            'dbxref_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'definition': ('django.db.models.fields.TextField', [], {}),
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '255'}),
            'url': ('django.db.models.fields.CharField', [], {'max_length': '255'})
        },
        u'ngsdbview.feature': {
            'Meta': {'object_name': 'Feature'},
            'aa_seq': ('django.db.models.fields.TextField', [], {}),
            'annotation': ('django.db.models.fields.CharField', [], {'max_length': '500', 'db_index': 'True'}),
            'chromosome': ('django.db.models.fields.CharField', [], {'max_length': '45', 'db_index': 'True'}),
            'feature_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'featuretype': ('django.db.models.fields.CharField', [], {'max_length': '40'}),
            'fmax': ('django.db.models.fields.IntegerField', [], {}),
            'fmin': ('django.db.models.fields.IntegerField', [], {}),
            'geneid': ('django.db.models.fields.CharField', [], {'max_length': '100', 'db_index': 'True'}),
            'geneproduct': ('django.db.models.fields.CharField', [], {'max_length': '500', 'db_index': 'True'}),
            'genome': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Genome']"}),
            'na_seq': ('django.db.models.fields.TextField', [], {}),
            'phase': ('django.db.models.fields.CharField', [], {'max_length': '5'}),
            'strand': ('django.db.models.fields.IntegerField', [], {}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now_add': 'True', 'blank': 'True'}),
            'time_data_modified': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.geneidmap': {
            'Meta': {'object_name': 'Geneidmap'},
            'db_soruce_current': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'db_soruce_previous': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'db_version_current': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'db_version_previous': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'geneid_current': ('django.db.models.fields.CharField', [], {'max_length': '50', 'db_index': 'True'}),
            'geneid_previous': ('django.db.models.fields.CharField', [], {'max_length': '50', 'db_index': 'True'}),
            'geneidmap_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.genome': {
            'Meta': {'object_name': 'Genome'},
            'genome_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'organism': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Organism']"}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'svnpath': ('django.db.models.fields.TextField', [], {'blank': 'True'}),
            'svnrevision': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'version': ('django.db.models.fields.CharField', [], {'max_length': '45'})
        },
        u'ngsdbview.genotype': {
            'Meta': {'object_name': 'Genotype'},
            'genotype': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '45'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '45', 'blank': 'True'})
        },
        u'ngsdbview.growthphase': {
            'Meta': {'object_name': 'Growthphase'},
            'growthphase': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '100'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '400', 'blank': 'True'})
        },
        u'ngsdbview.library': {
            'Meta': {'object_name': 'Library'},
            'author': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Author']"}),
            'collaborator': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Collaborator']"}),
            'downloaddate': ('django.db.models.fields.DateField', [], {}),
            'fastqalias': ('django.db.models.fields.CharField', [], {'max_length': '1000'}),
            'fastqname': ('django.db.models.fields.CharField', [], {'max_length': '1000'}),
            'fastqpath': ('django.db.models.fields.CharField', [], {'max_length': '1025'}),
            'flowcell': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'library_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'librarycode': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '25', 'db_index': 'True'}),
            'librarysize': ('django.db.models.fields.IntegerField', [], {}),
            'librarytype': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Librarytype']"}),
            'lifestage': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Lifestage']"}),
            'notes': ('django.db.models.fields.CharField', [], {'max_length': '400'}),
            'organism': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Organism']"}),
            'phenotype': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Phenotype']"}),
            'protocol': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Protocol']"})
        },
        u'ngsdbview.libraryfile': {
            'Meta': {'object_name': 'Libraryfile'},
            'file': ('django.db.models.fields.files.FileField', [], {'max_length': '100'}),
            'library': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Library']"}),
            'libraryfile_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': "'qc'", 'max_length': '1000'})
        },
        u'ngsdbview.libraryprop': {
            'Meta': {'object_name': 'Libraryprop'},
            'cvterm': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Cvterm']"}),
            'library': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Library']"}),
            'libraryprop_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'value': ('django.db.models.fields.TextField', [], {})
        },
        u'ngsdbview.librarytype': {
            'Meta': {'object_name': 'Librarytype'},
            'librarytype_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.TextField', [], {'max_length': '400', 'blank': 'True'}),
            'type': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '25'})
        },
        u'ngsdbview.lifestage': {
            'Meta': {'object_name': 'Lifestage'},
            'lifestage': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '45'}),
            'lifestage_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '400'})
        },
        u'ngsdbview.organism': {
            'Meta': {'object_name': 'Organism'},
            'genus': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'isolate': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'organism_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'organismcode': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '10'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'speceis': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'strain': ('django.db.models.fields.CharField', [], {'max_length': '45'})
        },
        u'ngsdbview.phenotype': {
            'Meta': {'object_name': 'Phenotype'},
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '45'}),
            'phenotype': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '45'}),
            'phenotype_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'ngsdbview.protocol': {
            'Meta': {'object_name': 'Protocol'},
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '50'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '400'}),
            'protocol_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'sopfile': ('django.db.models.fields.files.FileField', [], {'max_length': '100'})
        },
        u'ngsdbview.result': {
            'Meta': {'object_name': 'Result'},
            'analysispath': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'author': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Author']"}),
            'genome': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Genome']"}),
            'is_current': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'is_obsolete': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'libraries': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['ngsdbview.Library']", 'symmetrical': 'False'}),
            'notes': ('django.db.models.fields.TextField', [], {'default': 'None'}),
            'result_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.resultfile': {
            'Meta': {'object_name': 'Resultfile'},
            'file': ('django.db.models.fields.files.FileField', [], {'max_length': '100'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': "'result'", 'max_length': '1000'}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'resultfile_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'ngsdbview.resultprop': {
            'Meta': {'object_name': 'Resultprop'},
            'cvterm': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Cvterm']"}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'resultprop_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'value': ('django.db.models.fields.TextField', [], {})
        },
        u'ngsdbview.resultraw': {
            'Meta': {'object_name': 'Resultraw'},
            'chromosome': ('django.db.models.fields.CharField', [], {'max_length': '250', 'db_index': 'True'}),
            'majorstrand': ('django.db.models.fields.IntegerField', [], {}),
            'negstrandcount': ('django.db.models.fields.IntegerField', [], {}),
            'position': ('django.db.models.fields.IntegerField', [], {'db_index': 'True'}),
            'posstrandcount': ('django.db.models.fields.IntegerField', [], {}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'resultraw_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'}),
            'totalcount': ('django.db.models.fields.IntegerField', [], {})
        },
        u'ngsdbview.resultslgene': {
            'Meta': {'object_name': 'Resultslgene'},
            'antisensereadcount': ('django.db.models.fields.IntegerField', [], {}),
            'antisensesitecount': ('django.db.models.fields.IntegerField', [], {}),
            'cdsend': ('django.db.models.fields.IntegerField', [], {}),
            'cdsstart': ('django.db.models.fields.IntegerField', [], {}),
            'chromosome': ('django.db.models.fields.CharField', [], {'max_length': '25'}),
            'geneid': ('django.db.models.fields.CharField', [], {'max_length': '45', 'db_index': 'True'}),
            'genestrand': ('django.db.models.fields.IntegerField', [], {}),
            'putative5utr': ('django.db.models.fields.IntegerField', [], {}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'resultslgene_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'sensedownsitecount': ('django.db.models.fields.IntegerField', [], {}),
            'sensereadcount': ('django.db.models.fields.IntegerField', [], {}),
            'sensesitecount': ('django.db.models.fields.IntegerField', [], {}),
            'senseupsitecount': ('django.db.models.fields.IntegerField', [], {}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.resultslsite': {
            'Meta': {'object_name': 'Resultslsite'},
            'cdsend': ('django.db.models.fields.IntegerField', [], {}),
            'cdsstart': ('django.db.models.fields.IntegerField', [], {}),
            'chromosome': ('django.db.models.fields.CharField', [], {'max_length': '15', 'db_index': 'True'}),
            'geneid': ('django.db.models.fields.CharField', [], {'max_length': '45', 'db_index': 'True'}),
            'genestrand': ('django.db.models.fields.IntegerField', [], {}),
            'intervallength': ('django.db.models.fields.IntegerField', [], {}),
            'position': ('django.db.models.fields.IntegerField', [], {'db_index': 'True'}),
            'putative5utr': ('django.db.models.fields.IntegerField', [], {}),
            'rank': ('django.db.models.fields.FloatField', [], {'db_index': 'True'}),
            'readcount': ('django.db.models.fields.IntegerField', [], {}),
            'readstrand': ('django.db.models.fields.CharField', [], {'max_length': '5'}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'resultslsite_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'slid': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'slpercent': ('django.db.models.fields.DecimalField', [], {'max_digits': '10', 'decimal_places': '7'}),
            'time_data_loaded': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'})
        },
        u'ngsdbview.seqtech': {
            'Meta': {'object_name': 'Seqtech'},
            'name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '100'}),
            'notes': ('django.db.models.fields.TextField', [], {'blank': 'True'}),
            'seqtech_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'ngsdbview.software': {
            'Meta': {'object_name': 'Software'},
            'algorithm': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'notes': ('django.db.models.fields.TextField', [], {'default': 'None'}),
            'software_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'sourceuri': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'version': ('django.db.models.fields.CharField', [], {'max_length': '255'})
        },
        u'ngsdbview.userprofile': {
            'Meta': {'object_name': 'UserProfile'},
            'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'libraries': ('django.db.models.fields.related.ManyToManyField', [], {'default': "['AH006']", 'to': u"orm['ngsdbview.Library']", 'symmetrical': 'False'}),
            'user': ('django.db.models.fields.related.OneToOneField', [], {'to': u"orm['auth.User']", 'unique': 'True'})
        }
    }

    complete_apps = ['ngsdbview']