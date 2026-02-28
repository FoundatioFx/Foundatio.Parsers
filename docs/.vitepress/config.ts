import { defineConfig } from 'vitepress'
import llmstxt from 'vitepress-plugin-llms'
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(defineConfig({
  title: 'Foundatio Parsers',
  description: 'Extensible Lucene-style query parser with Elasticsearch and SQL support',
  base: '/',
  ignoreDeadLinks: true,
  vite: {
    plugins: [
      llmstxt({
        title: 'Foundatio Parsers Documentation',
        ignoreFiles: ['node_modules/**', '.vitepress/**']
      })
    ]
  },
  head: [
    ['link', { rel: 'icon', href: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/main/media/foundatio-icon.png', type: 'image/png' }],
    ['meta', { name: 'theme-color', content: '#3c8772' }]
  ],
  themeConfig: {
    logo: {
      light: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg',
      dark: 'https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio-dark-bg.svg'
    },
    siteTitle: 'Parsers',
    nav: [
      { text: 'Guide', link: '/guide/what-is-foundatio-parsers' },
      { text: 'GitHub', link: 'https://github.com/FoundatioFx/Foundatio.Parsers' }
    ],
    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'What is Foundatio.Parsers?', link: '/guide/what-is-foundatio-parsers' },
            { text: 'Getting Started', link: '/guide/getting-started' }
          ]
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Query Syntax', link: '/guide/query-syntax' },
            { text: 'Aggregation Syntax', link: '/guide/aggregation-syntax' },
            { text: 'Field Aliases', link: '/guide/field-aliases' },
            { text: 'Query Includes', link: '/guide/query-includes' },
            { text: 'Validation', link: '/guide/validation' },
            { text: 'Visitors', link: '/guide/visitors' }
          ]
        },
        {
          text: 'Advanced Topics',
          items: [
            { text: 'Custom Visitors', link: '/guide/custom-visitors' },
            { text: 'Elasticsearch Integration', link: '/guide/elastic-query-parser' },
            { text: 'Elasticsearch Mappings', link: '/guide/elastic-mappings' },
            { text: 'SQL Integration', link: '/guide/sql-query-parser' },
            { text: 'Troubleshooting', link: '/guide/troubleshooting' }
          ]
        }
      ]
    },
    socialLinks: [
      { icon: 'github', link: 'https://github.com/FoundatioFx/Foundatio.Parsers' },
      { icon: 'discord', link: 'https://discord.gg/6HxgFCx' }
    ],
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2026 Foundatio'
    },
    editLink: {
      pattern: 'https://github.com/FoundatioFx/Foundatio.Parsers/edit/main/docs/:path'
    },
    search: {
      provider: 'local'
    }
  },
  markdown: {
    lineNumbers: false,
    codeTransformers: [
      {
        name: 'snippet-transformer',
        preprocess(code, options) {
          return code
        }
      }
    ]
  }
}))
