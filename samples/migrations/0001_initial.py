# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'Genome'
        db.create_table(u'samples_genome', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_code', self.gf('django.db.models.fields.CharField')(unique=True, max_length=10)),
            ('genus', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('species', self.gf('django.db.models.fields.CharField')(max_length=45)),
            ('strain', self.gf('django.db.models.fields.CharField')(max_length=45, blank=True)),
            ('isolate', self.gf('django.db.models.fields.CharField')(max_length=45, blank=True)),
            ('source', self.gf('django.db.models.fields.CharField')(max_length=100, blank=True)),
            ('dbxref', self.gf('django.db.models.fields.CharField')(max_length=25)),
        ))
        db.send_create_signal(u'samples', ['Genome'])

        # Adding model 'Bioproject'
        db.create_table(u'samples_bioproject', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('bioproject_code', self.gf('django.db.models.fields.CharField')(unique=True, max_length=12)),
            ('organisms', self.gf('django.db.models.fields.CharField')(max_length=100, blank=True)),
            ('sharepoint_projectcode', self.gf('django.db.models.fields.CharField')(max_length=12, blank=True)),
            ('notes', self.gf('django.db.models.fields.TextField')(max_length=400, blank=True)),
        ))
        db.send_create_signal(u'samples', ['Bioproject'])

        # Adding model 'Biosample'
        db.create_table(u'samples_biosample', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('biosample_code', self.gf('django.db.models.fields.CharField')(unique=True, max_length=12)),
            ('organisms', self.gf('django.db.models.fields.CharField')(max_length=100, blank=True)),
            ('notes', self.gf('django.db.models.fields.TextField')(max_length=400, blank=True)),
        ))
        db.send_create_signal(u'samples', ['Biosample'])

        # Adding model 'Protocol'
        db.create_table(u'samples_protocol', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('protocol_name', self.gf('django.db.models.fields.CharField')(unique=True, max_length=50)),
            ('protocol_file', self.gf('django.db.models.fields.files.FileField')(max_length=100, blank=True)),
            ('protocol_link', self.gf('django.db.models.fields.URLField')(max_length=1000, blank=True)),
            ('notes', self.gf('django.db.models.fields.CharField')(default=None, max_length=400)),
        ))
        db.send_create_signal(u'samples', ['Protocol'])

        # Adding model 'Library'
        db.create_table(u'samples_library', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('library_code', self.gf('django.db.models.fields.CharField')(db_index=True, unique=True, max_length=10, blank=True)),
            ('author', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.authors', to=orm['ngsdbview.Author'])),
            ('collaborator', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.collaborator', to=orm['ngsdbview.Collaborator'])),
            ('bioproject', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['samples.Bioproject'])),
            ('biosample', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['samples.Biosample'])),
            ('organism', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.organism', to=orm['ngsdbview.Organism'])),
            ('lifestage', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.lifestage', to=orm['ngsdbview.Lifestage'])),
            ('growthphase', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.growthphase', to=orm['ngsdbview.Growthphase'])),
            ('phenotype', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.phenotype', to=orm['ngsdbview.Phenotype'])),
            ('genotype', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.genotype', to=orm['ngsdbview.Genotype'])),
            ('source', self.gf('django.db.models.fields.CharField')(max_length=100)),
            ('treatment', self.gf('django.db.models.fields.CharField')(max_length=100)),
            ('collected_on', self.gf('django.db.models.fields.DateField')(null=True, blank=True)),
            ('collected_at', self.gf('django.db.models.fields.CharField')(max_length=100, blank=True)),
            ('collected_by', self.gf('django.db.models.fields.CharField')(max_length=50, blank=True)),
            ('sample_notes', self.gf('django.db.models.fields.TextField')()),
            ('is_clonal', self.gf('django.db.models.fields.BooleanField')(default=False)),
            ('librarytype', self.gf('django.db.models.fields.related.ForeignKey')(related_name='ngsdbview.librarytype', to=orm['ngsdbview.Librarytype'])),
            ('template_material', self.gf('django.db.models.fields.CharField')(max_length=200)),
            ('protocol', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['samples.Protocol'])),
            ('protocol_notes', self.gf('django.db.models.fields.TextField')(default='None', blank=True)),
            ('library_creation_date', self.gf('django.db.models.fields.DateField')(null=True, blank=True)),
            ('submitted_for_sequencing_on', self.gf('django.db.models.fields.DateField')(null=True, blank=True)),
            ('sequence_downloaded_on', self.gf('django.db.models.fields.DateField')(null=True, blank=True)),
            ('flowcell_number', self.gf('django.db.models.fields.CharField')(max_length=15, blank=True)),
            ('lane_number', self.gf('django.db.models.fields.CharField')(max_length=3, blank=True)),
            ('index_sequence', self.gf('django.db.models.fields.CharField')(max_length=25, blank=True)),
            ('experiment_notes', self.gf('django.db.models.fields.TextField')(blank=True)),
            ('reference_genome', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['samples.Genome'])),
            ('reference_genome_version', self.gf('django.db.models.fields.CharField')(default='Latest', max_length=50, blank=True)),
            ('note_for_analysis', self.gf('django.db.models.fields.TextField')(blank=True)),
            ('date_created', self.gf('django.db.models.fields.DateTimeField')(auto_now_add=True, blank=True)),
            ('date_modified', self.gf('django.db.models.fields.DateTimeField')(auto_now=True, blank=True)),
            ('author_modified', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['auth.User'])),
        ))
        db.send_create_signal(u'samples', ['Library'])


    def backwards(self, orm):
        # Deleting model 'Genome'
        db.delete_table(u'samples_genome')

        # Deleting model 'Bioproject'
        db.delete_table(u'samples_bioproject')

        # Deleting model 'Biosample'
        db.delete_table(u'samples_biosample')

        # Deleting model 'Protocol'
        db.delete_table(u'samples_protocol')

        # Deleting model 'Library'
        db.delete_table(u'samples_library')


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
        u'samples.bioproject': {
            'Meta': {'object_name': 'Bioproject'},
            'bioproject_code': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '12'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.TextField', [], {'max_length': '400', 'blank': 'True'}),
            'organisms': ('django.db.models.fields.CharField', [], {'max_length': '100', 'blank': 'True'}),
            'sharepoint_projectcode': ('django.db.models.fields.CharField', [], {'max_length': '12', 'blank': 'True'})
        },
        u'samples.biosample': {
            'Meta': {'object_name': 'Biosample'},
            'biosample_code': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '12'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.TextField', [], {'max_length': '400', 'blank': 'True'}),
            'organisms': ('django.db.models.fields.CharField', [], {'max_length': '100', 'blank': 'True'})
        },
        u'samples.genome': {
            'Meta': {'object_name': 'Genome'},
            'dbxref': ('django.db.models.fields.CharField', [], {'max_length': '25'}),
            'genus': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'isolate': ('django.db.models.fields.CharField', [], {'max_length': '45', 'blank': 'True'}),
            'reference_code': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '10'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '100', 'blank': 'True'}),
            'species': ('django.db.models.fields.CharField', [], {'max_length': '45'}),
            'strain': ('django.db.models.fields.CharField', [], {'max_length': '45', 'blank': 'True'})
        },
        u'samples.library': {
            'Meta': {'object_name': 'Library'},
            'author': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.authors'", 'to': u"orm['ngsdbview.Author']"}),
            'author_modified': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['auth.User']"}),
            'bioproject': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['samples.Bioproject']"}),
            'biosample': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['samples.Biosample']"}),
            'collaborator': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.collaborator'", 'to': u"orm['ngsdbview.Collaborator']"}),
            'collected_at': ('django.db.models.fields.CharField', [], {'max_length': '100', 'blank': 'True'}),
            'collected_by': ('django.db.models.fields.CharField', [], {'max_length': '50', 'blank': 'True'}),
            'collected_on': ('django.db.models.fields.DateField', [], {'null': 'True', 'blank': 'True'}),
            'date_created': ('django.db.models.fields.DateTimeField', [], {'auto_now_add': 'True', 'blank': 'True'}),
            'date_modified': ('django.db.models.fields.DateTimeField', [], {'auto_now': 'True', 'blank': 'True'}),
            'experiment_notes': ('django.db.models.fields.TextField', [], {'blank': 'True'}),
            'flowcell_number': ('django.db.models.fields.CharField', [], {'max_length': '15', 'blank': 'True'}),
            'genotype': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.genotype'", 'to': u"orm['ngsdbview.Genotype']"}),
            'growthphase': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.growthphase'", 'to': u"orm['ngsdbview.Growthphase']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'index_sequence': ('django.db.models.fields.CharField', [], {'max_length': '25', 'blank': 'True'}),
            'is_clonal': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'lane_number': ('django.db.models.fields.CharField', [], {'max_length': '3', 'blank': 'True'}),
            'library_code': ('django.db.models.fields.CharField', [], {'db_index': 'True', 'unique': 'True', 'max_length': '10', 'blank': 'True'}),
            'library_creation_date': ('django.db.models.fields.DateField', [], {'null': 'True', 'blank': 'True'}),
            'librarytype': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.librarytype'", 'to': u"orm['ngsdbview.Librarytype']"}),
            'lifestage': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.lifestage'", 'to': u"orm['ngsdbview.Lifestage']"}),
            'note_for_analysis': ('django.db.models.fields.TextField', [], {'blank': 'True'}),
            'organism': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.organism'", 'to': u"orm['ngsdbview.Organism']"}),
            'phenotype': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'ngsdbview.phenotype'", 'to': u"orm['ngsdbview.Phenotype']"}),
            'protocol': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['samples.Protocol']"}),
            'protocol_notes': ('django.db.models.fields.TextField', [], {'default': "'None'", 'blank': 'True'}),
            'reference_genome': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['samples.Genome']"}),
            'reference_genome_version': ('django.db.models.fields.CharField', [], {'default': "'Latest'", 'max_length': '50', 'blank': 'True'}),
            'sample_notes': ('django.db.models.fields.TextField', [], {}),
            'sequence_downloaded_on': ('django.db.models.fields.DateField', [], {'null': 'True', 'blank': 'True'}),
            'source': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'submitted_for_sequencing_on': ('django.db.models.fields.DateField', [], {'null': 'True', 'blank': 'True'}),
            'template_material': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'treatment': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'samples.protocol': {
            'Meta': {'object_name': 'Protocol'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'notes': ('django.db.models.fields.CharField', [], {'default': 'None', 'max_length': '400'}),
            'protocol_file': ('django.db.models.fields.files.FileField', [], {'max_length': '100', 'blank': 'True'}),
            'protocol_link': ('django.db.models.fields.URLField', [], {'max_length': '1000', 'blank': 'True'}),
            'protocol_name': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '50'})
        }
    }

    complete_apps = ['samples']