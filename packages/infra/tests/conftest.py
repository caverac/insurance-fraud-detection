"""Tests for CDK stacks."""

import aws_cdk as cdk
import pytest


@pytest.fixture
def app() -> cdk.App:
    """Create a CDK app for testing."""
    return cdk.App()


@pytest.fixture
def env() -> cdk.Environment:
    """Create a CDK environment for testing."""
    return cdk.Environment(account="123456789012", region="us-east-1")
