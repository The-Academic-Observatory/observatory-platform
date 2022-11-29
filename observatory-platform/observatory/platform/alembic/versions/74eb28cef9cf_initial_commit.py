"""Initial commit

Revision ID: 74eb28cef9cf
Revises:
Create Date: 2022-05-11 20:39:59.365335

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '74eb28cef9cf'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('bigquery_bytes_processed',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('project', sa.String(length=250), nullable=True),
    sa.Column('total', sa.BigInteger(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('organisation',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('project_id', sa.String(length=30), nullable=True),
    sa.Column('download_bucket', sa.String(length=222), nullable=True),
    sa.Column('transform_bucket', sa.String(length=222), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('table_type',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('type_id', sa.String(length=250), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('type_id')
    )
    op.create_table('workflow_type',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('type_id', sa.String(length=250), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('type_id')
    )
    op.create_table('dataset_type',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('type_id', sa.String(length=250), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('extra', sa.JSON(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('table_type_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['table_type_id'], ['table_type.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('type_id')
    )
    op.create_table('workflow',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('extra', sa.JSON(), nullable=True),
    sa.Column('tags', sa.String(length=250), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('organisation_id', sa.Integer(), nullable=True),
    sa.Column('workflow_type_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['organisation_id'], ['organisation.id'], ),
    sa.ForeignKeyConstraint(['workflow_type_id'], ['workflow_type.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('dataset',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=250), nullable=True),
    sa.Column('service', sa.String(length=250), nullable=False),
    sa.Column('address', sa.String(length=250), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.Column('workflow_id', sa.Integer(), nullable=False),
    sa.Column('dataset_type_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['dataset_type_id'], ['dataset_type.id'], ),
    sa.ForeignKeyConstraint(['workflow_id'], ['workflow.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('dataset_release',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=True),
    sa.Column('end_date', sa.DateTime(), nullable=True),
    sa.Column('dataset_id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.Column('modified', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('dataset_release')
    op.drop_table('dataset')
    op.drop_table('workflow')
    op.drop_table('dataset_type')
    op.drop_table('workflow_type')
    op.drop_table('table_type')
    op.drop_table('organisation')
    op.drop_table('bigquery_bytes_processed')
