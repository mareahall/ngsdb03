# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Deleting field 'SNP.alternateBase'
        db.delete_column(u'ngsdbview_snp', 'alternateBase')

        # Deleting field 'SNP.referenceBase'
        db.delete_column(u'ngsdbview_snp', 'referenceBase')

        # Adding field 'SNP.ref_base'
        db.add_column(u'ngsdbview_snp', 'ref_base',
                      self.gf('django.db.models.fields.CharField')(default='a', max_length=10),
                      keep_default=False)

        # Adding field 'SNP.alt_base'
        db.add_column(u'ngsdbview_snp', 'alt_base',
                      self.gf('django.db.models.fields.CharField')(default='c', max_length=10),
                      keep_default=False)


    def backwards(self, orm):
        # Adding field 'SNP.alternateBase'
        db.add_column(u'ngsdbview_snp', 'alternateBase',
                      self.gf('django.db.models.fields.CharField')(default='a', max_length=10),
                      keep_default=False)

        # Adding field 'SNP.referenceBase'
        db.add_column(u'ngsdbview_snp', 'referenceBase',
                      self.gf('django.db.models.fields.CharField')(default='c', max_length=10),
                      keep_default=False)

        # Deleting field 'SNP.ref_base'
        db.delete_column(u'ngsdbview_snp', 'ref_base')

        # Deleting field 'SNP.alt_base'
        db.delete_column(u'ngsdbview_snp', 'alt_base')


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
        u'ngsdbview.chromosome': {
            'Meta': {'object_name': 'Chromosome'},
            'chromosome_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'chromosome_name': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'genome_name': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Organism']", 'to_field': "'organismcode'"}),
            'genome_version': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'size': ('django.db.models.fields.IntegerField', [], {})
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
        u'ngsdbview.effect': {
            'Meta': {'object_name': 'Effect'},
            'effect_class': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'effect_id': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Effect_CV']"}),
            'effect_string': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'snp': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.SNP']"})
        },
        u'ngsdbview.effect_cv': {
            'Meta': {'object_name': 'Effect_CV'},
            'effect_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'effect_name': ('django.db.models.fields.CharField', [], {'max_length': '45'})
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
        u'ngsdbview.filter': {
            'Meta': {'object_name': 'Filter'},
            'filter_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'filter_result': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'snp': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.SNP']"})
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
        u'ngsdbview.snp': {
            'Meta': {'object_name': 'SNP'},
            'alt_base': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'chromosome': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Chromosome']"}),
            'heterozygosity': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'library': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Library']"}),
            'quality': ('django.db.models.fields.IntegerField', [], {}),
            'ref_base': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'snp_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'snptype': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.SNP_Type']"})
        },
        u'ngsdbview.snp_summary': {
            'Meta': {'object_name': 'SNP_Summary'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'level': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Summary_Level_CV']"}),
            'result': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Result']"}),
            'tag': ('django.db.models.fields.TextField', [], {}),
            'value': ('django.db.models.fields.TextField', [], {}),
            'value_type': ('django.db.models.fields.TextField', [], {})
        },
        u'ngsdbview.snp_type': {
            'Meta': {'object_name': 'SNP_Type'},
            'deletion': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'indel': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'is_snp': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'monomorphic': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'snptype_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'sv': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'transition': ('django.db.models.fields.BooleanField', [], {'default': 'False'})
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
        u'ngsdbview.statistics': {
            'Meta': {'object_name': 'Statistics'},
            'cv_value': ('django.db.models.fields.IntegerField', [], {}),
            'snp': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.SNP']"}),
            'stats_cvterm': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['ngsdbview.Statistics_cv']", 'to_field': "'cvterm'"}),
            'stats_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'ngsdbview.statistics_cv': {
            'Meta': {'object_name': 'Statistics_cv'},
            'cv_notes': ('django.db.models.fields.TextField', [], {}),
            'cvterm': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '20'}),
            'cvterm_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'ngsdbview.summary_level_cv': {
            'Meta': {'object_name': 'Summary_Level_CV'},
            'level_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'level_name': ('django.db.models.fields.CharField', [], {'max_length': '25'})
        },
        u'ngsdbview.userprofile': {
            'Meta': {'object_name': 'UserProfile'},
            'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'libraries': ('django.db.models.fields.related.ManyToManyField', [], {'default': "['AH006']", 'to': u"orm['ngsdbview.Library']", 'symmetrical': 'False'}),
            'user': ('django.db.models.fields.related.OneToOneField', [], {'to': u"orm['auth.User']", 'unique': 'True'})
        }
    }

    complete_apps = ['ngsdbview']